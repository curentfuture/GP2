import re
from datetime import datetime, timedelta
import feedparser
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler

LOG_PATH = "parser.log"

def setup_logger(path=LOG_PATH, level=logging.INFO):
    logger = logging.getLogger("parser")
    if logger.handlers:
        return logger
    logger.setLevel(level)
    fh = RotatingFileHandler(path, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

logger = setup_logger()

def log(msg):
    print(msg)
    try:
        logger.info(msg)
    except Exception:
        pass

period = [(y, m) for y in (2023, 2024, 2025) for m in range(1, (11 if y == 2025 else 13))]

companies = {
    "microsoft": {
        "variants": ["microsoft", "msft", "windows", "xbox", "azure", "linkedin", "office 365", "microsoft 365"],
        "core": ["acquire", "acquired", "acquisition", "buy", "purchase", "merge", "merger", "takeover",
                 "joint venture", "deal", "transaction", "investment", "invests", "invested", "raises",
                 "funding round", "wins", "secures", "signs"],
        "objects": ["company", "business", "asset", "subsidiary", "division", "unit", "stake", "equity",
                    "government", "ministry", "contract", "agreement", "client", "partner", "startup", "investment"],
        "exclude": ["stock", "forecast", "review", "opinion", "windows 10", "windows 11", "bug", "fix", "gaming", "update"]
    },
    "google": {
        "variants": ["google", "alphabet", "gcp", "youtube", "waymo", "android", "deepmind", "pixel", "gemini"],
        "core": ["acquir", "buy", "purchase", "invest", "funding", "raises", "merg", "deal", "contract",
                 "agreement", "transaction", "partners with", "order", "renewal", "renews"],
        "objects": ["cloud", "data center", "infrastructure", "capacity", "quantum", "ai model", "mobile",
                    "telecom", "advertising", "youtube", "waymo"],
        "exclude": ["stock", "forecast", "review", "opinion", "regulation", "antitrust", "launch", "update"]
    },
    "oracle": {
        "variants": ["oracle", "orcl", "oci", "oracle cloud", "fusion cloud", "netsuite", "cerner"],
        "core": ["sign", "ink", "win", "secure", "select", "choose", "deploy", "migrate", "expand", "order",
                 "acquire", "merge", "invest", "funding", "raise"],
        "objects": ["contract", "agreement", "deal", "partnership", "collaboration", "transaction", "order",
                    "tender", "client", "customer", "government", "public sector", "healthcare",
                    "data center", "ai infrastructure", "gpu", "capacity"],
        "exclude": ["price target", "stock", "analyst", "forecast", "review", "opinion"]
    },
    "meta": {
        "variants": ["meta", "facebook", "instagram", "whatsapp", "threads", "oculus", "quest", "reality labs", "llama"],
        "core": ["acquir", "buy", "purchase", "merg", "invest", "funding", "raises", "deal", "transaction",
                 "agreement", "contract", "award", "mou", "partnership"],
        "objects": ["ai", "llama", "model", "infrastructure", "cloud", "datacenter", "vr", "ar", "government",
                    "power", "energy", "ppa", "distribution"],
        "exclude": ["stock", "share price", "target", "analyst", "forecast", "sale", "discount", "review"]
    },
    "palantir": {
        "variants": ["palantir", "pltr", "gotham", "foundry", "apollo"],
        "core": ["acquire", "acquired", "deal", "contract", "wins", "secures", "award", "signs", "expands",
                 "invests", "investment", "funding"],
        "objects": ["government", "defense", "military", "army", "navy", "contract", "agreement", "agency",
                    "startup", "federal", "partner", "program", "intelligence"],
        "exclude": ["stock", "forecast", "review", "opinion", "price target"]
    },
    "nvidia": {
        "variants": ["nvidia", "nvda", "cuda", "rtx", "blackwell", "grace", "mellanox", "gpu"],
        "core": ["acquir", "buy", "purchase", "merg", "invest", "funding", "raises", "stake", "deal", "contract",
                 "agreement", "partners with", "order", "joint venture"],
        "objects": ["company", "startup", "factory", "datacenter", "chip", "semiconductor", "server", "cloud",
                    "ai", "infrastructure", "capacity"],
        "exclude": ["review", "benchmark", "gaming", "stock", "forecast", "rating", "visit", "pledges"]
    }
}

money = re.compile(r"\$?\d+(?:,\d{3})*(?:\.\d+)?\s?(million|billion|bn|m)", re.I)

def split_month(y, m, parts=12):
    s = datetime(y, m, 1)
    e = datetime(y + (m == 12), (m % 12) + 1, 1)
    step = max((e - s).days // parts, 1)
    res, cur = [], s
    while cur < e:
        nxt = min(cur + timedelta(days=step), e)
        res.append((cur, nxt))
        cur = nxt
    return res

def build_query(c, s, e):
    q = f"{c} acquisition OR merger OR invest OR deal OR contract OR partnership after:{s.date()} before:{e.date()}"
    return q.replace(" ", "+")

def is_deal(title, cfg):
    t = title.lower()
    if not any(v in t for v in cfg["variants"]): return False
    if any(ex in t for ex in cfg["exclude"]): return False
    core = sum(k in t for k in cfg["core"])
    obj = sum(o in t for o in cfg["objects"])
    if money.search(t): obj += 1
    return core >= 1 and obj >= 1

def fetch_range(c, cfg, s, e):
    url = f"https://news.google.com/rss/search?q={build_query(c, s, e)}&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        status = getattr(feed, "status", None)
        if getattr(feed, "bozo", 0):
            logger.warning(f"[{c}] {s.date()}→{e.date()} bozo=1 status={status} err={getattr(feed, 'bozo_exception', '')}")
        entries = getattr(feed, "entries", [])
        kept = [{"date": getattr(f, "published", None), "title": f.title, "url": f.link}
                for f in entries if is_deal(f.title, cfg)]
        logger.debug(f"[{c}] {s.date()}→{e.date()} entries={len(entries)} kept={len(kept)} status={status}")
        return kept
    except Exception as ex:
        logger.exception(f"[{c}] ERROR fetching {s.date()}→{e.date()} url={url}: {ex}")
        return []

def run_all():
    for c, cfg in companies.items():
        log(f"\n=== {c.upper()} — DEALS / CONTRACTS / INVESTMENTS — 2023 → Oct 2025 ===\n")
        all_rows = []
        for y, m in period:
            log(f"{y}-{m:02d}")
            for i, (s, e) in enumerate(split_month(y, m), 1):
                rows = fetch_range(c, cfg, s, e)
                all_rows += rows
                log(f"  [{i:02d}] {s.date()} → {e.date()} — {len(rows)}")
        df = pd.DataFrame(all_rows).drop_duplicates(["title", "url"]).reset_index(drop=True)
        out = f"{c}_deals_2023_2025.csv"
        df.to_csv(out, index=False)
        log(f"\nSaved {len(df)} → {out}")

if __name__ == "__main__":
    run_all()
