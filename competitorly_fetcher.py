
import re
from datetime import datetime, timedelta
import feedparser
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler

LOG_PATH = "parser_comp.log"

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
        "variants": ["microsoft", "msft", "azure", "windows", "office", "linkedin", "openai"],
        "rivals": ["google", "alphabet", "amazon", "aws", "meta", "facebook", "apple", "nvidia"],
        "themes": ["competition", "rivalry", "market share", "battle", "challenge", "vs", "versus",
                   "dominance", "leadership", "ai race", "enterprise ai", "cloud war"],
        "exclude": ["review", "bug", "patch", "update", "gaming", "xbox", "forecast", "stock"]
    },
    "google": {
        "variants": ["google", "alphabet", "youtube", "gcp", "waymo", "gemini"],
        "rivals": ["microsoft", "openai", "meta", "facebook", "amazon", "aws", "apple", "nvidia"],
        "themes": ["competition", "rival", "ai race", "dominance", "fight for", "market share",
                   "search war", "browser war", "ad war", "enterprise ai"],
        "exclude": ["review", "update", "tips", "pixel", "app", "game", "beta"]
    },
    "meta": {
        "variants": ["meta", "facebook", "instagram", "whatsapp", "threads", "oculus", "quest"],
        "rivals": ["tiktok", "snap", "youtube", "google", "apple", "microsoft"],
        "themes": ["competition", "social media war", "ad market", "engagement", "platform rivalry",
                   "ai race", "vr", "ar", "metaverse", "creator economy"],
        "exclude": ["review", "discount", "sale", "headset review", "game"]
    },
    "oracle": {
        "variants": ["oracle", "orcl", "oci", "oracle cloud", "fusion cloud", "netsuite"],
        "rivals": ["microsoft", "google", "aws", "amazon", "sap", "ibm", "salesforce"],
        "themes": ["competition", "rivalry", "cloud war", "enterprise software", "market share",
                   "ai infrastructure", "erp battle"],
        "exclude": ["stock", "forecast", "review", "update"]
    },
    "nvidia": {
        "variants": ["nvidia", "nvda", "cuda", "blackwell", "grace", "hopper", "mellanox"],
        "rivals": ["amd", "intel", "qualcomm", "google", "amazon", "meta", "microsoft"],
        "themes": ["competition", "rivalry", "ai chip", "gpu race", "semiconductor war", "dominance",
                   "supply chain", "leadership"],
        "exclude": ["gaming", "review", "benchmark", "fps", "graphics card"]
    },
    "palantir": {
        "variants": ["palantir", "pltr", "gotham", "foundry", "apollo"],
        "rivals": ["snowflake", "databricks", "c3.ai", "ibm", "oracle", "microsoft", "aws"],
        "themes": ["competition", "government contract", "ai platform", "defense tech", "rivalry",
                   "federal ai", "edge analytics", "contractor race"],
        "exclude": ["stock", "forecast", "review", "opinion"]
    }
}

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
    q = f"{c} competition OR rivalry OR market share OR vs OR ai race after:{s.date()} before:{e.date()}"
    return q.replace(" ", "+")

def is_competitor_news(title, cfg):
    t = title.lower()
    if not any(v in t for v in cfg["variants"]):
        return False
    if any(ex in t for ex in cfg["exclude"]):
        return False
    if not any(r in t for r in cfg["rivals"]):
        return False
    if any(th in t for th in cfg["themes"]):
        return True
    if re.search(r"\b(vs|versus|battle|race|fight)\b", t):
        return True
    return False

def fetch_range(c, cfg, s, e):
    url = f"https://news.google.com/rss/search?q={build_query(c, s, e)}&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        status = getattr(feed, "status", None)
        if getattr(feed, "bozo", 0):
            logger.warning(f"[{c}] {s.date()}→{e.date()} bozo=1 status={status} err={getattr(feed, 'bozo_exception', '')}")
        entries = getattr(feed, "entries", [])
        kept = [{"date": getattr(f, "published", None), "title": f.title, "url": f.link}
                for f in entries if is_competitor_news(f.title, cfg)]
        logger.debug(f"[{c}] {s.date()}→{e.date()} entries={len(entries)} kept={len(kept)} status={status}")
        return kept
    except Exception as ex:
        logger.exception(f"[{c}] ERROR fetching {s.date()}→{e.date()} url={url}: {ex}")
        return []

def run_all():
    for c, cfg in companies.items():
        log(f"\n=== {c.upper()} COMPETITORLY — 2023 → Oct 2025 ===\n")
        all_rows = []
        for y, m in period:
            log(f"{y}-{m:02d}")
            for i, (s, e) in enumerate(split_month(y, m), 1):
                rows = fetch_range(c, cfg, s, e)
                all_rows += rows
                log(f"  [{i:02d}] {s.date()} → {e.date()} — {len(rows)}")
        df = pd.DataFrame(all_rows).drop_duplicates(["title", "url"]).reset_index(drop=True)
        out = f"{c}_competitorly_2023_2025.csv"
        df.to_csv(out, index=False)
        log(f"\nSaved {len(df)} → {out}")

if __name__ == "__main__":
    run_all()

