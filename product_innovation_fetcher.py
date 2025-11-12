import re
from datetime import datetime, timedelta
import feedparser
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler

LOG_PATH = "parser_product.log"

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
    "microsoft": {
        "keys": ["microsoft", "windows", "azure", "office", "microsoft 365", "copilot", "bing", "surface", "github", "visual studio", ".net", "phi"],
        "ex": ["lawsuit", "stock", "forecast", "review", "game", "patch", "tips", "politic", "ban", "breach"],
        "accept": [
            r"azure.*(launch|platform|region|ai|ga|compute)",
            r"copilot.*(launch|ai|platform|enterprise|integration)",
            r"windows 1[12]|windows 12",
            r"surface.*(launch|release|hardware)",
            r"(office|microsoft 365).*(ai|copilot|platform)",
            r"github.*(copilot|ai|launch)",
            r"\.net.*(8|9|10)|c#.*(12|13|14)"
        ],
        "query": "(microsoft OR windows OR azure OR copilot OR office OR github OR surface)"
    },
    "google": {
        "keys": ["google", "alphabet", "android", "pixel", "gemini", "deepmind", "chrome", "workspace", "assistant", "gcp", "cloud"],
        "ex": ["lawsuit", "stock", "market", "review", "tips", "patch", "scandal", "outage", "ban", "hack"],
        "accept": [
            r"android (1[0-9]|2[0-9])", r"pixel [6-9]|[1-9][0-9]",
            r"gemini.*(model|launch|api|version)", r"deepmind.*(model|system|agent)",
            r"(gcp|google cloud).*(launch|platform|region|service)",
            r"workspace.*(ai|gemini|enterprise|platform)"
        ],
        "query": "(google OR gemini OR deepmind OR gcp OR android OR pixel)"
    },
    "meta": {
        "keys": ["meta", "facebook", "instagram", "whatsapp", "threads", "quest", "oculus", "llama", "meta ai"],
        "ex": ["lawsuit", "stock", "forecast", "review", "filter", "reel", "tips", "minor update"],
        "accept": [
            r"llama.*(launch|model|api)", r"meta ai.*(model|platform|assistant)",
            r"quest.*(launch|release|update)", r"oculus.*(launch|update)",
            r"ray[- ]ban.*meta.*(launch|release)",
            r"(whatsapp|instagram|messenger).*(ai|platform|major update)"
        ],
        "query": "(meta OR facebook OR instagram OR whatsapp OR quest OR oculus)"
    },
    "oracle": {
        "keys": ["oracle", "oci", "autonomous db", "heatwave", "fusion", "java", "cerner", "oracle health"],
        "ex": ["lawsuit", "stock", "scandal", "forecast", "review", "politic", "breach"],
        "accept": [
            r"oci.*(launch|region|platform|ai|service)",
            r"autonomous db|autonomous database", r"heatwave.*(launch|ai|lakehouse)",
            r"fusion.*(ai|upgrade|platform)", r"java.*(21|22|23|24).*release",
            r"oracle.*(ai|llm|agent|generative)", r"cerner.*(platform|ai|launch)"
        ],
        "query": "(oracle OR oci OR autonomous db OR heatwave OR fusion OR java)"
    },
    "nvidia": {
        "keys": ["nvidia", "nvda", "cuda", "dgx", "hgx", "hopper", "blackwell", "grace", "tensorrt", "nemo"],
        "ex": ["stock", "forecast", "review", "gaming", "fps", "benchmark", "leak", "lawsuit", "ban"],
        "accept": [
            r"blackwell|b200|gb200|gh200|gh300", r"dgx|hgx.*(platform|launch)",
            r"cuda.*(update|release)", r"tensorrt.*(llm|inference)",
            r"nemo.*(framework|model|release)", r"nvidia.*(ai platform|training|inference)",
            r"jetson|isaac|drive.*(platform|ai)"
        ],
        "query": "(nvidia OR cuda OR dgx OR hgx OR blackwell OR grace OR tensorrt)"
    },
    "palantir": {
        "keys": ["palantir", "aip", "foundry", "gotham", "apollo"],
        "ex": ["lawsuit", "stock", "forecast", "contract", "military", "scandal", "ban", "politic"],
        "accept": [
            r"aip.*(launch|capability|platform)", r"foundry.*(launch|platform|upgrade)",
            r"gotham.*(launch|platform)", r"apollo.*(deployment|launch)",
            r"palantir.*(ai|llm|agent|framework)"
        ],
        "query": "(palantir OR aip OR foundry OR gotham OR apollo)"
    }
}

def split_month(y, m, parts=12):
    s, e = datetime(y, m, 1), datetime(y + (m == 12), (m % 12) + 1, 1)
    step = max((e - s).days // parts, 1)
    return [(s + timedelta(days=i*step), min(s + timedelta(days=(i+1)*step), e)) for i in range(parts)]

def build_query(q, s, e):
    return f"{q} after:{s.date()} before:{e.date()}".replace(" ", "+")

def is_product_news(t, cfg):
    t = t.lower()
    if not any(k in t for k in cfg["keys"]): return False
    if any(re.search(p, t) for p in cfg["ex"]): return False
    return any(re.search(p, t) for p in cfg["accept"])

def fetch_range(cfg, s, e):
    url = f"https://news.google.com/rss/search?q={build_query(cfg['query'], s, e)}&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        status = getattr(feed, "status", None)
        if getattr(feed, "bozo", 0):
            logger.warning(f"[products] {s.date()}→{e.date()} bozo=1 status={status} err={getattr(feed, 'bozo_exception', '')}")
        entries = getattr(feed, "entries", [])
        kept = [{"date": getattr(f, "published", None), "title": f.title, "url": f.link}
                for f in entries if is_product_news(f.title, cfg)]
        logger.debug(f"[products] {s.date()}→{e.date()} entries={len(entries)} kept={len(kept)} status={status}")
        return kept
    except Exception as ex:
        logger.exception(f"[products] ERROR {s.date()}→{e.date()} url={url}: {ex}")
        return []

def run_all_products():
    for c, cfg in companies.items():
        log(f"\n=== {c.upper()} — PRODUCT / INNOVATION NEWS — Jan 2023 → Oct 2025 ===\n")
        all_rows = []
        for y in [2023, 2024, 2025]:
            for m in range(1, (11 if y == 2025 else 13)):
                log(f"{y}-{m:02d}")
                for i, (s, e) in enumerate(split_month(y, m), 1):
                    rows = fetch_range(cfg, s, e)
                    all_rows += rows
                    log(f"  [{i:02d}] {s.date()} → {e.date()} — {len(rows)}")
        df = pd.DataFrame(all_rows).drop_duplicates(["title", "url"]).reset_index(drop=True)
        out = f"{c}_products_2023jan_2025oct.csv"
        df.to_csv(out, index=False)
        log(f"\nSaved {len(df)} → {out}")

if __name__ == "__main__":
    run_all_products()

