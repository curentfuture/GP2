import os, io, datetime as dt
import pandas as pd
import requests
from config import FRED_KEY
import logging
from logging.handlers import RotatingFileHandler

LOG_PATH = "parser_macro.log"

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

START_DATE="2000-01-01"
END_DATE=dt.date.today().isoformat()
OUTDIR="data"
os.makedirs(OUTDIR,exist_ok=True)
# функция по работе с API FRED
def fetch_fred(series_id,start=START_DATE,end=None,rename_to=None):
    url="https://api.stlouisfed.org/fred/series/observations"
    params={"series_id":series_id,"api_key":FRED_KEY,"file_type":"json","observation_start":start}
    if end:
       params["observation_end"]=end
    try:
        r=requests.get(url,params=params,timeout=30)
        status=r.status_code
        js=r.json()
        df=pd.DataFrame(js["observations"])[["date","value"]]
        df["date"]=pd.to_datetime(df["date"])
        if rename_to:
          col = rename_to
        else:
          col = series_id.lower()
        df[col]=pd.to_numeric(df["value"].replace(".",pd.NA))
        out_df = df[["date",col]].dropna().sort_values("date").reset_index(drop=True)
        log(f"[FRED] {series_id} {start}→{end or 'today'} status={status} rows={len(out_df)}")
        return out_df
    except Exception as ex:
        col = rename_to if rename_to else series_id.lower()
        logger.exception(f"[FRED] ERROR series={series_id} {start}→{end or 'today'}: {ex}")
        return pd.DataFrame(columns=["date", col])

# функция по работе с API FRED
# качаем CSV со Stooq: берём Close по XAUUSD/XAGUSD(золото+серебро)
def fetch_stooq(ticker,rename_to,start=START_DATE):
    url=f"https://stooq.com/q/d/l/?s={ticker}&i=d"
    try:
        r=requests.get(url,timeout=60)
        status=r.status_code
        df=pd.read_csv(io.StringIO(r.text)).rename(columns={"Date":"date","Close":rename_to})
        df["date"]=pd.to_datetime(df["date"])
        df=df[["date",rename_to]].dropna()
        out_df = df[df["date"]>=pd.to_datetime(start)].sort_values("date").reset_index(drop=True)
        log(f"[STOOQ] {ticker} {start}→today status={status} rows={len(out_df)}")
        return out_df
    except Exception as ex:
        logger.exception(f"[STOOQ] ERROR ticker={ticker} {start}→today: {ex}")
        return pd.DataFrame(columns=["date", rename_to])

# берём FRED: нефть, ставка ФРС, Nasdaq, биткоин
wti=fetch_fred("DCOILWTICO",start=START_DATE,end=END_DATE,rename_to="wti_usd_bbl")# WTI, $/bbl
brent=fetch_fred("DCOILBRENTEU",start=START_DATE,end=END_DATE,rename_to="brent_usd_bbl")# Brent, $/bbl
ff = fetch_fred("DFF",  start=START_DATE, end=END_DATE, rename_to="fedfunds_daily_pct") # эффективная ставка по федеральным фондам США, %
ndq=fetch_fred("NASDAQCOM",start=START_DATE,end=END_DATE,rename_to="nasdaq_close") # Nasdaq Composite, $
btc=fetch_fred("CBBTCUSD",start=START_DATE,end=END_DATE,rename_to="btc_usd") # BTC/USD (Coinbase), $

# берём золото и серебро
gold=fetch_stooq("xauusd","xau_usd_oz",start=START_DATE)
silver=fetch_stooq("xagusd","xag_usd_oz",start=START_DATE)

# сохраняем отдельные наборы как есть
gold.to_csv(f"{OUTDIR}/gold_xauusd.csv",index=False); log(f"Saved {OUTDIR}/gold_xauusd.csv rows={len(gold)}")
silver.to_csv(f"{OUTDIR}/silver_xagusd.csv",index=False); log(f"Saved {OUTDIR}/silver_xagusd.csv rows={len(silver)}")
wti.to_csv(f"{OUTDIR}/oil_wti.csv",index=False); log(f"Saved {OUTDIR}/oil_wti.csv rows={len(wti)}")
brent.to_csv(f"{OUTDIR}/oil_brent.csv",index=False); log(f"Saved {OUTDIR}/oil_brent.csv rows={len(brent)}")
ff.to_csv(f"{OUTDIR}/fed_funds.csv",index=False); log(f"Saved {OUTDIR}/fed_funds.csv rows={len(ff)}")
ndq.to_csv(f"{OUTDIR}/nasdaq_close.csv",index=False); log(f"Saved {OUTDIR}/nasdaq_close.csv rows={len(ndq)}")
btc.to_csv(f"{OUTDIR}/btc_usd.csv",index=False); log(f"Saved {OUTDIR}/btc_usd.csv rows={len(btc)}")

#мерджим все воедино
panel=gold.merge(silver,on="date",how="outer")
panel=panel.merge(wti,on="date",how="outer")
panel=panel.merge(brent,on="date",how="outer")
panel=panel.merge(ff,on="date",how="outer")
panel=panel.merge(ndq,on="date",how="outer")
panel=panel.merge(btc,on="date",how="outer")
panel=panel.sort_values("date").set_index("date").reset_index()
panel.to_csv(f"{OUTDIR}/macro_joined_daily.csv",index=False)
log(f"Saved {OUTDIR}/macro_joined_daily.csv rows={len(panel)} cols={list(panel.columns)}")
