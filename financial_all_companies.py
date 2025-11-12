import re, feedparser, pandas as pd
from datetime import datetime, timedelta
from dateutil import parser as dp
import logging
from logging.handlers import RotatingFileHandler

LOG_PATH = "parser_financial.log"

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

def split_month(y, m, p=12):
    s = datetime(y, m, 1)
    e = datetime(y+(m==12), (m%12)+1, 1)
    d = (e-s).days//p or 1
    r, c = [], s
    for _ in range(p):
        n = min(c+timedelta(days=d), e)
        r.append((c,n))
        c=n
        if c>=e: break
    return r

def period():
    return [(y,m) for y in range(2023,2026) for m in range(1,13) if not (y==2025 and m>10)]

def fetch(q,s,e,f):
    u=f"https://news.google.com/rss/search?q={q.replace(' ','+')}+after:{s.date()}+before:{e.date()}&hl=en-US&gl=US&ceid=US:en"
    try:
        fd=feedparser.parse(u)
        status=getattr(fd,"status",None)
        if getattr(fd,"bozo",0):
            logger.warning(f"[financial] {s.date()}→{e.date()} bozo=1 status={status} err={getattr(fd,'bozo_exception','')}")
        a=[]
        for x in getattr(fd,"entries",[]):
            t=x.title; sm=getattr(x,'summary','')
            if f(t,sm): a.append({'date':getattr(x,'published',None),'title':t,'url':x.link,'summary':sm})
        logger.debug(f"[financial] {s.date()}→{e.date()} entries={len(getattr(fd,'entries',[]))} kept={len(a)} status={status}")
        return a
    except Exception as ex:
        logger.exception(f"[financial] ERROR {s.date()}→{e.date()} url={u}: {ex}")
        return []

def run(name,query,flt,out):
    log(f"=== {name.upper()} FINANCIAL — 2023 → Oct 2025 ===")
    all=[]
    for y,m in period():
        log(f"{y}-{m:02d}")
        for i,(s,e) in enumerate(split_month(y,m),1):
            r=fetch(f"{name} {query}",s,e,flt)
            log(f"  [{i:02d}] {s.date()}→{e.date()} {len(r)}")
            all+=r
    df=pd.DataFrame(all).drop_duplicates(['title','url'])
    df.to_csv(out,index=False,encoding='utf-8')
    log(f"Saved {out} {len(df)}")

def f_google(t,s):
    x=f"{t.lower()} {s.lower()}"
    if not any(k in x for k in ['google','alphabet']): return False
    if any(re.search(p,x) for p in [r'review',r'pixel',r'android',r'update']): return False
    return any(k in x for k in ['earnings','q1','q2','q3','q4','guidance','revenue','profit','growth','forecast','results'])

def f_microsoft(t,s):
    x=f"{t.lower()} {s.lower()}"
    if not any(k in x for k in ['microsoft','msft','azure','windows']): return False
    if any(re.search(p,x) for p in [r'gaming',r'xbox',r'update']): return False
    return any(k in x for k in ['earnings','revenue','profit','outlook','forecast','q1','q2','q3','q4'])

def f_meta(t,s):
    x=f"{t.lower()} {s.lower()}"
    if not any(k in x for k in ['meta','facebook','instagram','whatsapp','oculus']): return False
    if any(re.search(p,x) for p in [r'vr',r'headset',r'gaming']): return False
    return any(k in x for k in ['earnings','revenue','profit','results','quarter','growth'])

def f_oracle(t,s):
    x=f"{t.lower()} {s.lower()}"
    if not any(k in x for k in ['oracle','orcl','oci','netsuite']): return False
    return any(k in x for k in ['earnings','fiscal','quarter','revenue','profit','guidance','forecast'])

def f_palantir(t,s):
    x=f"{t.lower()} {s.lower()}"
    if not any(k in x for k in ['palantir','pltr','gotham','foundry']): return False
    return any(k in x for k in ['earnings','revenue','profit','forecast','results','guidance'])

def f_nvidia(t,s):
    x=f"{t.lower()} {s.lower()}"
    if not any(k in x for k in ['nvidia','nvda']): return False
    if any(re.search(p,x) for p in [r'gpu',r'geforce',r'gaming']): return False
    return any(k in x for k in ['earnings','revenue','profit','quarter','forecast','results'])

def main():
    run('google','earnings OR revenue OR profit',f_google,'google_financial_2023_2025_oct.csv')
    run('microsoft','earnings OR revenue OR profit',f_microsoft,'microsoft_financial_2023_2025_oct.csv')
    run('meta','earnings OR revenue OR profit',f_meta,'meta_financial_2023_2025_oct.csv')
    run('oracle','earnings OR revenue OR profit',f_oracle,'oracle_financial_2023_2025_oct.csv')
    run('palantir','earnings OR revenue OR profit',f_palantir,'palantir_financial_2023_2025_oct.csv')
    run('nvidia','earnings OR revenue OR profit',f_nvidia,'nvidia_financial_2023_2025_oct.csv')

if __name__=='__main__': main()
