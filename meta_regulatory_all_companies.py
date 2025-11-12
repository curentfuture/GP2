import re
import feedparser
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser as dateparser
import logging
from logging.handlers import RotatingFileHandler

LOG_PATH = "parser_regulatory.log"

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

companies = {
    "meta": {
        "tags": ["meta", "facebook", "instagram", "whatsapp", "oculus", "quest", "reality labs", "threads", "llama"],
        "exclude": [r"headset", r"vr", r"ar", r"gaming", r"review", r"update", r"ai model", r"feature"],
        "terms": ["ftc", "doj", "lawsuit", "court", "settlement", "privacy", "regulatory", "regulator", "compliance", "monopoly", "probe", "investigation", "fine", "penalty", "eu", "european commission", "digital markets act", "gdpr"],
    },
    "palantir": {
        "tags": ["palantir", "pltr", "gotham", "foundry", "apollo"],
        "exclude": [r"price target", r"valuation", r"demo", r"conference", r"rumor"],
        "terms": ["government", "dod", "defense", "military", "contract", "federal", "security", "cia", "nsa", "surveillance", "regulatory", "lawsuit", "court", "investigation", "probe", "compliance", "restriction", "sanction"],
    },
    "microsoft": {
        "tags": ["microsoft", "msft", "windows", "azure", "office", "teams", "bing", "copilot"],
        "exclude": [r"xbox", r"gaming", r"launch", r"event", r"hardware", r"ai model"],
        "terms": ["regulatory", "antitrust", "lawsuit", "court", "ftc", "doj", "sec", "eu commission", "probe", "investigation", "government", "contract", "defense", "sanction", "restriction"],
    },
    "oracle": {
        "tags": ["oracle", "orcl", "oci", "fusion", "cerner", "oracle cloud"],
        "exclude": [r"conference", r"event", r"earnings", r"crypto"],
        "terms": ["regulatory", "government", "contract", "award", "federal", "va", "dod", "army", "navy", "air force", "privacy", "compliance", "lawsuit", "antitrust", "investigation", "probe"],
    },
    "nvidia": {
        "tags": ["nvidia", "nvda"],
        "exclude": [r"gpu", r"rtx", r"geforce", r"gaming", r"benchmark", r"launch"],
        "terms": ["regulatory", "lawsuit", "government", "doj", "ftc", "sec", "probe", "investigation", "restriction", "export ban", "license", "sanction", "compliance"],
    },
    "google": {
        "tags": ["google", "alphabet", "youtube", "waymo", "deepmind", "gcp"],
        "exclude": [r"android", r"pixel", r"maps", r"chrome", r"review", r"launch", r"app"],
        "terms": ["antitrust", "regulatory", "regulator", "lawsuit", "ftc", "doj", "government", "probe", "investigation", "privacy", "compliance", "gdpr", "fine", "settlement", "penalty"],
    }
}

def split_month(y, m, parts=12):
    s, e = datetime(y, m, 1), datetime(y + (m == 12), (m % 12) + 1, 1)
    step = max((e - s).days // parts, 1)
    return [(s + timedelta(days=i * step), min(s + timedelta(days=(i + 1) * step), e)) for i in range(parts)]

def build_query(c, s, e):
    q = f"{c} (regulatory OR lawsuit OR investigation OR government OR antitrust OR privacy) after:{s.date()} before:{e.date()}"
    return q.replace(" ", "+")

def is_regulatory(title, summary, cfg):
    t = f"{title.lower()} {summary.lower()}"
    if not any(k in t for k in cfg["tags"]): return False
    if any(re.search(p, t) for p in cfg["exclude"]): return False
    return any(term in t for term in cfg["terms"])

def fetch_range(name, cfg, s, e):
    url = f"https://news.google.com/rss/search?q={build_query(name, s, e)}&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        status = getattr(feed, "status", None)
        if getattr(feed, "bozo", 0):
            logger.warning(f"[regulatory] {name} {s.date()}→{e.date()} bozo=1 status={status} err={getattr(feed, 'bozo_exception', '')}")
        entries = getattr(feed, "entries", [])
        rows = []
        for f in entries:
            if is_regulatory(f.title, getattr(f, "summary", ""), cfg):
                try:
                    d = dateparser.parse(getattr(f, "published", ""))
                except Exception:
                    d = None
                rows.append({"date": d, "title": f.title, "summary": getattr(f, "summary", ""), "url": f.link})
        logger.debug(f"[regulatory] {name} {s.date()}→{e.date()} entries={len(entries)} kept={len(rows)} status={status}")
        return rows
    except Exception as ex:
        logger.exception(f"[regulatory] {name} ERROR {s.date()}→{e.date()} url={url}: {ex}")
        return []

def run_all_regulatory():
    for name, cfg in companies.items():
        log(f"\n=== {name.upper()} — REGULATORY / LEGAL / GOVERNMENT — 2023 → Oct 2025 ===\n")
        all_rows = []
        for y in [2023, 2024, 2025]:
            for m in range(1, (11 if y == 2025 else 13)):
                log(f"{y}-{m:02d}")
                for i, (s, e) in enumerate(split_month(y, m), 1):
                    rows = fetch_range(name, cfg, s, e)
                    all_rows += rows
                    log(f"  [{i:02d}] {s.date()} → {e.date()} — {len(rows)}")
        df = pd.DataFrame(all_rows).drop_duplicates(["title", "url"]).reset_index(drop=True)
        out = f"{name}_regulatory_2023_2025_oct.csv"
        df.to_csv(out, index=False)
        log(f"\nSaved {len(df)} → {out}")

if __name__ == "__main__":
    run_all_regulatory()
