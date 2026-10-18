"""Acme Appliances ledger Q&A service.

POST {"question": "..."}  ->  {"answer": ...}

Data rules (from the API's own notes):
  * Business dates use Asia/Kolkata (UTC+05:30, no DST).
  * An order id can appear more than once; the row with the latest updated_at is current.
  * Only status 'paid' counts as revenue.
Money is converted to USD with the API's /rates and rounded to the cent.
"""
import calendar
import json
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

BASE = os.environ.get(
    "LEDGER_BASE",
    "https://exam.sanand.workers.dev/questionData?email=22ds2000079%40ds.study.iitm.ac.in"
    "&quizSign=JWIq7osgrm1y%2BtNTH5LOfttRabnV%2B5mBtNpNBC%2FmIC%2F%2BHC07gfp9kGgVsVsGN9d4ZFpJBs%2BQI6rspQohInrAE6uWEd0FFjHFEGbMjyatCVGXYgb4xnmjXJZQVO84DWHrH1oE1z82upgNw4ip77X3eGSD59suVzNAvBEHPdKChSoWlgzJWzw2WxSA%2F3IBWUoV0WanjQlfN9axlc2R0AjHQrUYpMW9FlP3V%2B12%2BtcXiXu5%2BbG5MYK26lKYxDue0vUWrB1g1DNiY%2FjkHc0hu7Ww9UFEmI5riGFauTK53QBJ80KTlwQFKCy88WtAxPLbV43FVcrBA14BfsJ8u0%2F1WknF9w%3D%3D"
    "&questionId=q-ledger-agent-server",
)
HERE = Path(__file__).resolve().parent
IST = timezone(timedelta(hours=5, minutes=30))
REFRESH_SECONDS = 600

# ---------------------------------------------------------------- data loading

STATE = {"orders": [], "rates": {"USD": 1.0, "EUR": 1.1, "INR": 0.01143}, "loaded_at": 0.0}
LOCK = threading.Lock()


def _get_json(path, extra=""):
    with urlopen(f"{BASE}&path={path}{extra}", timeout=20) as r:
        return json.loads(r.read().decode())


def _get_text(path):
    with urlopen(f"{BASE}&path={path}", timeout=20) as r:
        return r.read().decode()


def _parse_ts(s):
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def _parse_ndjson(text):
    out = []
    for line in text.splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def _fetch_all_pages():
    first = _get_json("%2Forders", "&page=1")
    rows = list(first.get("orders", []))
    pages = int(first.get("pages", 1))
    with ThreadPoolExecutor(max_workers=8) as ex:
        for data in ex.map(lambda p: _get_json("%2Forders", f"&page={p}"), range(2, pages + 1)):
            rows.extend(data.get("orders", []))
    return rows


def _build(raw_rows, rates):
    """Keep the latest version of each order id and precompute fields."""
    latest = {}
    for r in raw_rows:
        try:
            upd = _parse_ts(r["updated_at"])
        except Exception:
            continue
        cur = latest.get(r["id"])
        if cur is None or upd > cur[0]:
            latest[r["id"]] = (upd, r)
    orders = []
    for upd, r in latest.values():
        created = _parse_ts(r["created_at"]).astimezone(IST)
        cur = str(r.get("currency", "USD")).upper()
        orders.append({
            **r,
            "status": str(r.get("status", "")).strip().lower(),
            "usd": float(r["amount"]) * float(rates.get(cur, 1.0)),
            "date": created.date(),
            "upd_date": upd.astimezone(IST).date(),
        })
    return orders


def load_data():
    raw, rates = [], dict(STATE["rates"])
    try:
        rates = {k.upper(): float(v) for k, v in _get_json("%2Frates")["usd_per_unit"].items()}
    except Exception as e:
        print("rates fetch failed:", e)
    try:
        raw.extend(_fetch_all_pages())
    except Exception as e:
        print("pages fetch failed:", e)
    try:
        raw.extend(_parse_ndjson(_get_text("%2Fexport")))
    except Exception as e:
        print("export fetch failed:", e)
    if not raw:  # offline fallback: bundled snapshot
        fallback = HERE / "export.ndjson"
        if fallback.exists():
            raw = _parse_ndjson(fallback.read_text())
    if raw:
        with LOCK:
            STATE["orders"] = _build(raw, rates)
            STATE["rates"] = rates
            STATE["loaded_at"] = time.time()
        print(f"loaded {len(STATE['orders'])} current orders")


def _refresher():
    while True:
        time.sleep(REFRESH_SECONDS)
        load_data()


# ---------------------------------------------------------------- question parsing

MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
MONTHS.update({m.lower(): i for i, m in enumerate(calendar.month_abbr) if m})
MONTHS["sept"] = 9
MONTH_RE = "|".join(sorted(MONTHS, key=len, reverse=True))
CURRENCY_WORDS = {"eur": "EUR", "euro": "EUR", "euros": "EUR", "inr": "INR", "rupee": "INR",
                  "rupees": "INR", "usd": "USD", "dollar": "USD", "dollars": "USD"}


def _norm(s):
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def _find_names(q, names):
    """Return names mentioned in q (handles plurals like 'air fryers')."""
    qn = f" {_norm(q)} "
    found = []
    for name in sorted(names, key=len, reverse=True):
        n = _norm(name)
        if re.search(rf" {re.escape(n)}(s|es)? ", qn):
            found.append(name)
            qn = qn.replace(f" {n}", " ")
    return found


def _year_near(q, default):
    m = re.search(r"\b(20\d\d)\b", q)
    return int(m.group(1)) if m else default


def _parse_date_str(s, default_year):
    s = s.strip().rstrip(",.")
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = re.match(rf"({MONTH_RE})\.?\s+(\d{{1,2}})(?!\d)(?:st|nd|rd|th)?(?:,?\s*(\d{{4}}))?$", s, re.I)
    if m:
        return date(int(m.group(3) or default_year), MONTHS[m.group(1).lower()], int(m.group(2)))
    m = re.match(rf"(\d{{1,2}})(?:st|nd|rd|th)?\s+(?:of\s+)?({MONTH_RE})\.?,?(?:\s+(\d{{4}}))?$", s, re.I)
    if m:
        return date(int(m.group(3) or default_year), MONTHS[m.group(2).lower()], int(m.group(1)))
    return None


def _month_range(y, m):
    return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])


def parse_period(q, default_year):
    """Return (start, end) inclusive dates, or None for all time."""
    ql = q.lower()
    y = _year_near(q, default_year)
    date_pat = rf"(\d{{4}}-\d{{1,2}}-\d{{1,2}}|(?:{MONTH_RE})\.?\s+\d{{1,2}}(?!\d)(?:st|nd|rd|th)?(?:,?\s*\d{{4}})?|(?<!\d)\d{{1,2}}(?:st|nd|rd|th)?\s+(?:of\s+)?(?:{MONTH_RE})\b\.?,?(?:\s+\d{{4}})?)"

    # explicit date range: between X and Y / from X to Y / X through Y
    m = re.search(rf"(?:(?:between|from)\s+)?{date_pat}\s*(?:and|to|through|till|until|-|–)\s*{date_pat}", q, re.I)
    if m:
        a, b = _parse_date_str(m.group(1), y), _parse_date_str(m.group(2), y)
        if a and b:
            return a, b
    # month range: between January and March 2026 / from Jan to Mar
    m = re.search(rf"(?:between|from)\s+({MONTH_RE})\.?(?:\s+(\d{{4}}))?\s+(?:and|to|through|till|until|-)\s+({MONTH_RE})\.?(?:\s+(\d{{4}}))?", q, re.I)
    if m:
        y1 = int(m.group(2) or m.group(4) or y)
        y2 = int(m.group(4) or y1)
        return _month_range(y1, MONTHS[m.group(1).lower()])[0], _month_range(y2, MONTHS[m.group(3).lower()])[1]
    # since / after / before a date
    m = re.search(rf"\b(since|after|on or after|before|prior to|up to|until|through)\s+{date_pat}", q, re.I)
    if m:
        d = _parse_date_str(m.group(2), y)
        if d:
            w = m.group(1).lower()
            if w in ("since", "on or after"):
                return d, date(9999, 12, 31)
            if w == "after":
                return d + timedelta(days=1), date(9999, 12, 31)
            if w in ("before", "prior to"):
                return date(1, 1, 1), d - timedelta(days=1)
            return date(1, 1, 1), d
    # single date: on March 5, 2026 / 2026-03-05
    m = re.search(date_pat, q, re.I)
    if m:
        d = _parse_date_str(m.group(1), y)
        if d:
            return d, d
    # quarter
    m = re.search(r"\bq([1-4])\b", ql) or re.search(r"\b(first|second|third|fourth)\s+quarter", ql)
    if m:
        qn = {"first": 1, "second": 2, "third": 3, "fourth": 4}.get(m.group(1), None) or int(m.group(1))
        return _month_range(y, 3 * qn - 2)[0], _month_range(y, 3 * qn)[1]
    # half year
    m = re.search(r"\bh([12])\b", ql) or re.search(r"\b(first|second)\s+half", ql)
    if m:
        h = 1 if m.group(1) in ("1", "first") else 2
        return _month_range(y, 6 * h - 5)[0], _month_range(y, 6 * h)[1]
    # month (+ optional year), e.g. "March 2026", "Mar-2026", "2026-03"
    m = re.search(r"\b(20\d\d)-(\d{1,2})\b(?!-)", q)
    if m:
        return _month_range(int(m.group(1)), int(m.group(2)))
    m = re.search(rf"\b({MONTH_RE})\b\.?", q, re.I)
    if m and not (m.group(1).lower() == "may" and re.search(r"\bmay\s+(i|we|you)\b", ql)):
        return _month_range(y, MONTHS[m.group(1).lower()])
    # whole year
    m = re.search(r"\b(20\d\d)\b", q)
    if m:
        return date(int(m.group(1)), 1, 1), date(int(m.group(1)), 12, 31)
    return None


def parse_question(q, products, regions, customers):
    ql = q.lower()
    f = {}
    f["products"] = _find_names(q, products)
    f["regions"] = _find_names(q, regions)
    f["customers"] = [c for c in customers if re.search(rf"\b{re.escape(c)}\b", q, re.I)]
    # currency filter: only when an order currency is clearly meant (not the output unit "in USD")
    cur = set()
    for w, c in CURRENCY_WORDS.items():
        for mm in re.finditer(rf"\b{w}\b", ql):
            if c != "USD":
                cur.add(c)  # EUR / INR mentioned => filter on order currency
                continue
            # USD is usually just the output unit; only filter when it describes the orders
            before = ql[max(0, mm.start() - 30):mm.start()]
            after = ql[mm.end():mm.end() + 20]
            if re.search(r"(paid|priced|denominated|billed|placed|booked|transacted)\s+in\s+$", before) \
                    or re.search(r"^\s*-?\s*(denominated|orders?|transactions?|sales|currency orders)\b", after):
                cur.add(c)
    f["currencies"] = sorted(cur)
    f["period"] = parse_period(q, 2026)
    return f


# ---------------------------------------------------------------- answering

def money(x):
    return round(x + 1e-9, 2)


def answer(q):
    with LOCK:
        orders = STATE["orders"]
    products = sorted({o["product"] for o in orders})
    regions = sorted({o["region"] for o in orders})
    customers = sorted({o["customer"] for o in orders})
    ql = q.lower()
    f = parse_question(q, products, regions, customers)
    # 'at least' / 'at most' are not superlatives ("customers with at least one order")
    ql = re.sub(r"\bat (least|most)\b", " ", ql)
    counting = re.search(r"^\s*(how many|how much|what is the (total )?(number|count|sum)|what was the (total )?(number|count|sum)|count|number of|total)\b", ql)

    # which status set?
    refund_words = re.search(r"refund", ql)
    void_words = re.search(r"\b(void|voided|cancel+ed|cancellations?)\b", ql)
    revenue_words = re.search(r"\b(revenue|sales|sold|earn|earned|income|paid|turnover|takings|gmv|spent|spend|bought|purchased?)\b", ql)
    net = re.search(r"\bnet\b", ql)

    if re.search(r"\b(including|regardless|irrespective|any status|all statuses|every status|of any status)\b", ql):
        statuses = {"paid", "refunded", "void"}
    elif net and refund_words:
        statuses = None  # handled specially
    elif refund_words:
        statuses = {"refunded"}
    elif void_words:
        statuses = {"void"}
    elif revenue_words:
        statuses = {"paid"}
    elif re.search(r"\b(all|total number of|any status|regardless)\b", ql):
        statuses = {"paid", "refunded", "void"}
    else:
        statuses = {"paid"}

    def keep(o, stat=statuses):
        if stat is not None and o["status"] not in stat:
            return False
        if f["products"] and o["product"] not in f["products"]:
            return False
        if f["regions"] and o["region"] not in f["regions"]:
            return False
        if f["customers"] and o["customer"] not in f["customers"]:
            return False
        if f["currencies"] and o["currency"] not in f["currencies"]:
            return False
        if f["period"] and not (f["period"][0] <= o["date"] <= f["period"][1]):
            return False
        return True

    rows = [o for o in orders if keep(o)]

    # metric
    by_units = re.search(r"\b(units?|quantity|quantities|qty|pieces|items)\b", ql)
    # "how many (US) dollars ..." asks for money, not a count
    money_words = re.search(r"\bhow (many|much) (us |u\.s\. |american )?(dollars?|usd|bucks)\b|\$|\bworth\b|\bvalue\b", ql)
    by_count = re.search(r"\b(how many|number of|count|most orders|fewest orders|frequent|most often)\b", ql) \
        and not by_units and not money_words
    want_avg = re.search(r"\b(average|avg|mean|typical)\b", ql)

    def metric(rs):
        if want_avg:
            if not rs:
                return 0
            if by_units:
                return round(sum(int(o["qty"]) for o in rs) / len(rs), 2)
            return money(sum(o["usd"] for o in rs) / len(rs))
        if by_units:
            return sum(int(o["qty"]) for o in rs)
        if by_count:
            return len(rs)
        return money(sum(o["usd"] for o in rs))

    # group-by questions: which product/region/customer/month is top/bottom
    group = None
    qn = _norm(ql)
    superlative = re.search(r"\b(top|best|worst|highest|lowest|most|least|largest|smallest|biggest|leading|"
                            r"greatest|fewest|maximum|minimum|max|min|grossing|bottom)\b", qn)
    if not counting and (superlative or re.search(r"^(which|what) (product|region|customer|client|month|currency)\b", qn)):
        dims = [("product", r"products?|items?|appliances?|skus?|models?"),
                ("region", r"regions?|areas?|territor(?:y|ies)|zones?"),
                ("customer", r"customers?|clients?|buyers?|accounts?|shoppers?"),
                ("month", r"months?"),
                ("currency", r"currenc(?:y|ies)")]
        filter_words = {_norm(x) for x in regions + products} | {c.lower() for c in customers}
        cands = []
        for name, pat in dims:
            for mm in re.finditer(rf"\b({pat})\b", qn):
                prev = qn[:mm.start()].split()
                prev2 = " ".join(prev[-2:])
                if (prev and prev[-1] in filter_words) or prev2 in filter_words:
                    continue  # e.g. "North region", "Air Fryer product" = a filter, not the thing asked
                if prev and prev[-1] in ("per", "each", "every", "this", "that", "same") and name == "month":
                    continue
                cands.append((mm.start(), name))
        if cands:
            group = min(cands)[1]
    if group:
        key = {"product": lambda o: o["product"], "region": lambda o: o["region"],
               "customer": lambda o: o["customer"], "currency": lambda o: o["currency"],
               "month": lambda o: o["date"].strftime("%B %Y")}[group]
        buckets = defaultdict(list)
        for o in rows:
            buckets[key(o)].append(o)
        if not buckets:
            return None
        lowest = re.search(r"\b(lowest|least|fewest|worst|smallest|minimum|bottom)\b", ql)
        scored = sorted(((metric(v), k) for k, v in buckets.items()), key=lambda t: (t[0], t[1]))
        best = scored[0] if lowest else max(scored, key=lambda t: (t[0], [-ord(c) for c in t[1]]))
        return best[1]

    if statuses is None:  # net revenue = paid - refunded
        paid = [o for o in orders if keep(o, {"paid"})]
        refd = [o for o in orders if keep(o, {"refunded"})]
        return money(sum(o["usd"] for o in paid) - sum(o["usd"] for o in refd))

    if re.search(r"\b(how many|number of|count)\s+(\w+\s+){0,2}(distinct|unique|different)?\s*(customers?|clients?|buyers?)\b", ql):
        return len({o["customer"] for o in rows})
    if re.search(r"\b(how many|number of|count)\s+(\w+\s+){0,2}(distinct|unique|different)\s+(products?|items?)\b", ql):
        return len({o["product"] for o in rows})
    return metric(rows)


# ---------------------------------------------------------------- web app

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
def _startup():
    load_data()
    threading.Thread(target=_refresher, daemon=True).start()


@app.post("/")
@app.post("/{path:path}")
async def ask(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    q = str((body or {}).get("question", "")).strip()
    if not q:
        return {"answer": None, "error": "Send JSON like {\"question\": \"...\"}"}
    try:
        return {"answer": answer(q)}
    except Exception as e:
        return {"answer": None, "error": str(e)}


@app.get("/")
@app.get("/health")
def health():
    return {"status": "ok", "orders": len(STATE["orders"]), "loaded_at": STATE["loaded_at"]}
