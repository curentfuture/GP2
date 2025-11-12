import re
from datetime import datetime, timedelta
import feedparser
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler

LOG_PATH = "parser_scandals.log"

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
        "keys": ["meta", "facebook", "instagram", "whatsapp", "threads", "oculus", "quest", "reality labs"],
        "ex": [
            r"update", r"launch", r"release", r"feature", r"ai", r"model", r"tool",
            r"reels", r"quest", r"oculus", r"threads.*growth", r"whatsapp.*update",
            r"review", r"glasses", r"ray-ban"
        ],
        "accept": [
            r"scandal", r"controversy", r"backlash", r"outrage", r"probe", r"investigation",
            r"lawsuit", r"privacy", r"data leak", r"breach", r"hacked", r"class action",
            r"teen", r"mental health", r"harassment", r"extremism", r"bias", r"hate speech"
        ],
    },
    "palantir": {
        "keys": ["palantir", "foundry", "gotham", "apollo"],
        "ex": [
            r"ai", r"launch", r"platform", r"update", r"demo", r"forecast", r"revenue", r"stock",
            r"contract", r"partnership", r"deal"
        ],
        "accept": [
            r"scandal", r"controversy", r"backlash", r"outrage", r"lawsuit", r"probe",
            r"surveillance", r"privacy", r"rights", r"ethics", r"civil liberties",
            r"immigration", r"military", r"war crimes", r"gaza", r"israel", r"dod", r"ice"
        ],
    },
    "microsoft": {
        "keys": ["microsoft", "msft", "windows", "azure", "office", "xbox", "linkedin", "github", "copilot"],
        "ex": [
            r"earnings", r"stock", r"forecast", r"update", r"ai model", r"partner", r"contract",
            r"preview", r"demo", r"cloud growth", r"launch"
        ],
        "accept": [
            r"lawsuit", r"probe", r"investigation", r"antitrust", r"privacy", r"tracking",
            r"hack", r"breach", r"outage", r"backlash", r"controversy", r"concern",
            r"monopoly", r"rights", r"harassment", r"layoff", r"toxic", r"china", r"russia"
        ],
    },
    "oracle": {
        "keys": ["oracle", "orcl", "oci", "fusion", "netsuite", "cerner"],
        "ex": [
            r"earnings", r"stock", r"forecast", r"launch", r"ai model", r"platform",
            r"partner", r"deal", r"integration", r"deployment"
        ],
        "accept": [
            r"lawsuit", r"probe", r"investigation", r"privacy", r"antitrust", r"monopoly",
            r"backlash", r"concern", r"critic", r"surveillance", r"data broker", r"rights",
            r"outage", r"breach", r"hack", r"layoff", r"harassment", r"tiktok"
        ],
    },
    "nvidia": {
        "keys": ["nvidia", "nvda", "geforce", "rtx", "cuda", "h100", "a100", "gh200", "blackwell"],
        "ex": [
            r"review", r"benchmark", r"fps", r"launch", r"update", r"gaming", r"stock",
            r"revenue", r"forecast", r"price target"
        ],
        "accept": [
            r"lawsuit", r"probe", r"investigation", r"sanction", r"export ban", r"china",
            r"antitrust", r"security", r"breach", r"hack", r"fraud", r"illegal",
            r"controversy", r"ai.*risk", r"supply crisis", r"layoff"
        ],
    },
    "google": {
        "keys": ["google", "alphabet", "youtube", "android", "deepmind", "gemini", "waymo", "gcp", "chrome"],
        "ex": [
            r"launch", r"update", r"release", r"feature", r"review", r"pixel",
            r"deal", r"contract", r"agreement", r"earnings", r"stock", r"forecast"
        ],
        "accept": [
            r"privacy", r"tracking", r"surveillance", r"gdpr", r"antitrust", r"monopoly",
            r"probe", r"investigation", r"lawsuit", r"youtube.*(harm|violation)",
            r"gemini.*(bias|racism)", r"layoff", r"harassment", r"retaliation",
            r"election", r"censor", r"bias", r"hack", r"outage"
        ],
    }
}

def split_month(y, m, parts=12):
    s, e = datetime(y, m, 1), datetime(y + (m == 12), (m % 12) + 1, 1)
    step = max((e - s).days // parts, 1)
    return [(s + timedelta(days=i * step), min(s + timedelta(days=(i + 1) * step), e)) for i in range(parts)]

def build_query(c, s, e):
    q = f"{c} (lawsuit OR controversy OR scandal OR investigation OR backlash OR probe OR privacy OR data breach OR hack OR rights) after:{s.date()} before:{e.date()}"
    return q.replace(" ", "+")

def is_scandal(title, cfg):
    t = title.lower()
    if not any(k in t for k in cfg["keys"]): return False
    if any(re.search(p, t) for p in cfg["ex"]): return False
    return any(re.search(p, t) for p in cfg["accept"])

def fetch_range(name, cfg, s, e):
    url = f"https://news.google.com/rss/search?q={build_query(name, s, e)}&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        status = getattr(feed, "status", None)
        if getattr(feed, "bozo", 0):
            logger.warning(f"[scandals] {name} {s.date()}→{e.date()} bozo=1 status={status} err={getattr(feed, 'bozo_exception', '')}")
        entries = getattr(feed, "entries", [])
        kept = [{"date": getattr(f, "published", None), "title": f.title, "url": f.link}
                for f in entries if is_scandal(f.title, cfg)]
        logger.debug(f"[scandals] {name} {s.date()}→{e.date()} entries={len(entries)} kept={len(kept)} status={status}")
        return kept
    except Exception as ex:
        logger.exception(f"[scandals] {name} ERROR {s.date()}→{e.date()} url={url}: {ex}")
        return []

def run_all_scandals():
    for name, cfg in companies.items():
        log(f"\n=== {name.upper()} — SCANDALS / CONTROVERSIES — 2023 → Oct 2025 ===\n")
        all_rows = []
        for y in [2023, 2024, 2025]:
            for m in range(1, (11 if y == 2025 else 13)):
                log(f"{y}-{m:02d}")
                for i, (s, e) in enumerate(split_month(y, m), 1):
                    rows = fetch_range(name, cfg, s, e)
                    all_rows += rows
                    log(f"  [{i:02d}] {s.date()} → {e.date()} — {len(rows)}")
        df = pd.DataFrame(all_rows).drop_duplicates(["title", "url"]).reset_index(drop=True)
        out = f"{name}_scandals_2023jan_2025oct.csv"
        df.to_csv(out, index=False)
        log(f"\nSaved {len(df)} → {out}")

if __name__ == "__main__":
    run_all_scandals()
