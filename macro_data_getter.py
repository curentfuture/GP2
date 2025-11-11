import os, io, datetime as dt
import pandas as pd
import requests
from config import FRED_KEY

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
    r=requests.get(url,params=params,timeout=30)
    js=r.json()
    df=pd.DataFrame(js["observations"])[["date","value"]]
    df["date"]=pd.to_datetime(df["date"])
    if rename_to:
      col = rename_to
    else:
      col = series_id.lower()
    df[col]=pd.to_numeric(df["value"].replace(".",pd.NA))
    return df[["date",col]].dropna().sort_values("date").reset_index(drop=True)
# функция по работе с API FRED
# качаем CSV со Stooq: берём Close по XAUUSD/XAGUSD(золото+серебро)
def fetch_stooq(ticker,rename_to,start=START_DATE):
    url=f"https://stooq.com/q/d/l/?s={ticker}&i=d"
    r=requests.get(url,timeout=60)
    df=pd.read_csv(io.StringIO(r.text)).rename(columns={"Date":"date","Close":rename_to})
    df["date"]=pd.to_datetime(df["date"])
    df=df[["date",rename_to]].dropna()
    return df[df["date"]>=pd.to_datetime(start)].sort_values("date").reset_index(drop=True)

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
gold.to_csv(f"{OUTDIR}/gold_xauusd.csv",index=False)
silver.to_csv(f"{OUTDIR}/silver_xagusd.csv",index=False)
wti.to_csv(f"{OUTDIR}/oil_wti.csv",index=False)
brent.to_csv(f"{OUTDIR}/oil_brent.csv",index=False)
ff.to_csv(f"{OUTDIR}/fed_funds.csv",index=False)
ndq.to_csv(f"{OUTDIR}/nasdaq_close.csv",index=False)
btc.to_csv(f"{OUTDIR}/btc_usd.csv",index=False)

#мерджим все воедино
panel=gold.merge(silver,on="date",how="outer")
panel=panel.merge(wti,on="date",how="outer")
panel=panel.merge(brent,on="date",how="outer")
panel=panel.merge(ff,on="date",how="outer")
panel=panel.merge(ndq,on="date",how="outer")
panel=panel.merge(btc,on="date",how="outer")
panel=panel.sort_values("date").set_index("date").reset_index()
panel.to_csv(f"{OUTDIR}/macro_joined_daily.csv",index=False)
