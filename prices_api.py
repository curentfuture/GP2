import os,io
import pandas as pd
import requests
from datetime import datetime

OUTDIR = "data"
os.makedirs(OUTDIR, exist_ok=True)

START_DATE = "2015-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# Список компаний для обработки
companies = [
    "googl.us",
    "msft.us", 
    "orcl.us",
    "pltr.us",
    "meta.us",
    "nvda.us"
]

for company in companies:
    url = f"https://stooq.com/q/d/l/?s={company}&i=d"
    csv = requests.get(url, timeout=30).text
    df = pd.read_csv(io.StringIO(csv)).rename(
        columns={
            "Date": "date",
            "Open": "open",
            "Close": "close",
            "Volume": "volume",
        }
    )

    # Оставляем только нужные колонки
    df = df[["date", "open", "close", "volume"]]

    # Преобразуем дату и фильтруем по периоду
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= START_DATE) & (df["date"] <= END_DATE)].sort_values("date")

    # Вычисляем дельту между ценой закрытия и открытия в процентах
    df["change_pct"] = (df["close"] / df["open"] - 1) * 100

    # Создаем имя файла
    filename = f"{company.replace('.us', '')}_stock_prices.csv"
    df.to_csv(os.path.join(OUTDIR, filename), index=False)
    print(f"Данные сохранены в {OUTDIR}/{filename}")
    print(f"Период: {START_DATE} - {END_DATE}")
    print(f"Записей: {len(df)}")