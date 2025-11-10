import re
from datetime import datetime, timedelta

import feedparser
import pandas as pd
import requests
from bs4 import BeautifulSoup

company = "nvidia"
text_limit = 800

# финансовые ключевые слова (semi-strict)
fin_keywords = [
    "earnings", "revenue", "sales", "profit", "loss", "forecast",
    "guidance", "results", "quarter", "q1", "q2", "q3", "q4",
    "eps", "net income", "fiscal", "financial report", "operating income",
]

# фильтр ненужных новостей
exclude_patterns = [
    r"\b(ai|gpu|chip|product|launch|data center|h100|blackwell|conference|partnership|rumor|announcement|event|demo)\b",
    r"\b(ai|gpu|chip|product|launch|data center|h100|blackwell|analyst|forecast|rumor|conference|ceo|partnership|crypto|bitcoin|prediction|price target|upgrade|downgrade|buy|sell)\b",
]


# забирает текст статьи и обрезает до limit
def get_article_text(url, limit=text_limit):
    try:
        headers = {"user-agent": "Mozilla/5.0"}
        response = requests.get(url, timeout=8, headers=headers)
        if response.status_code != 200:
            return ""

        soup = BeautifulSoup(response.text, "html.parser")
        paragraphs = soup.find_all("p")
        text = " ".join(p.get_text(" ", strip=True) for p in paragraphs)
        return text[:limit]
    except Exception:
        return ""


# делит месяц на parts отрезков дат
def split_month(year, month, parts=12):
    start = datetime(year, month, 1)

    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)

    total_days = (end - start).days
    step = max(total_days // parts, 1)

    ranges = []
    current = start

    for _ in range(parts):
        next_end = current + timedelta(days=step)
        if next_end > end:
            next_end = end

        ranges.append((current, next_end))
        current = next_end

        if current >= end:
            break

    return ranges


# собирает строку запроса в google news с фин-ключами и датами
def build_search_query(company_name, start, end):
    base_query = f"{company_name} earnings OR revenue OR profit OR results OR forecast"
    date_filters = f" after:{start.date()} before:{end.date()}"
    return (base_query + date_filters).replace(" ", "+")


# проверяет, является ли новость финансовой (semi-strict) по nvidia
def is_financial_type1(title, snippet):
    full = f"{title.lower()} {snippet.lower()}"

    for pattern in exclude_patterns:
        if re.search(pattern, full):
            return False

    fin_hits = sum(keyword in full for keyword in fin_keywords)

    has_numbers = bool(
        re.search(r"[\$€£]?\d+(\.\d+)?\s?(billion|million|bn|m|%)?", full)
    )

    mentions_company = "nvidia" in full or "nvda" in full

    if mentions_company and fin_hits >= 1 and (has_numbers or fin_hits >= 2):
        return True

    return False


# забирает и фильтрует новости за указанный период
def fetch_chunk(start, end):
    query = build_search_query(company, start, end)
    url = f"https://news.google.com/rss/search?q={query}&hl=en-us&gl=us&ceid=us:en"

    feed = feedparser.parse(url)
    rows = []

    for entry in feed.entries:
        title = entry.title
        link = entry.link
        snippet = get_article_text(link)

        if is_financial_type1(title, snippet):
            rows.append({
                "date": getattr(entry, "published", None),
                "title": title,
                "url": link,
                "snippet": snippet,
            })

    return rows


# возвращает список (year, month) за последние 3 года до текущего месяца включительно
def get_last_3_year_months():
    end = datetime.utcnow()
    start = end - timedelta(days=365 * 3)

    year = start.year
    month = start.month

    months = []
    while year < end.year or (year == end.year and month <= end.month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    return months


# возвращает df с финансовыми новостями за последние 3 года
def run_financial_last_3_years():
    all_rows = []
    months = get_last_3_year_months()

    for year, month in months:
        ranges = split_month(year, month, parts=12)
        for start, end in ranges:
            all_rows.extend(fetch_chunk(start, end))

    if not all_rows:
        return pd.DataFrame(columns=["date", "title", "url", "snippet", "category"])

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["title", "url"]).reset_index(drop=True)

    df["category"] = "financial_results"

    return df


if __name__ == "__main__":
    df_financial = run_financial_last_3_years()

