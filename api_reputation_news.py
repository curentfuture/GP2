import re
from datetime import datetime, timedelta
from difflib import SequenceMatcher

import feedparser
import pandas as pd
import requests
from bs4 import BeautifulSoup

company = "nvidia"
text_limit = 800

# ключевые слова по репутации / имиджу (широкий охват)
reputation_keywords = [
    "scandal", "controversy", "backlash", "outcry",
    "harassment", "discrimination", "misconduct",
    "toxic workplace", "whistleblower", "data breach",
    "breach", "hacked", "cyberattack", "exposed data",
    "resignation", "steps down", "public apology",
    "award", "top ranking", "best employer", "prestigious",
    "recognized", "global recognition", "won", "acclaimed",
    "viral", "trending", "buzz", "public reaction",
    "influential", "image", "reputation", "public image",
    "ceo", "jensen huang", "executive",
]

# тени типов 1–4 (для отсева не-reputation medium-кейсов)
legal_shadow = [
    "lawsuit", "litigation", "antitrust", "regulator",
    "court", "sec", "ftc", "sanction", "export", "probe",
]

product_shadow = [
    "launch", "release", "unveil", "product",
    "chip", "gpu", "architecture", "platform",
    "technology", "roadmap",
]

finance_shadow = [
    "revenue", "earnings", "forecast", "guidance",
    "profit", "loss", "quarter", "q1", "q2", "q3", "q4",
]

mna_shadow = [
    "acquisition", "acquire", "merger", "buyout",
    "investment", "stake", "partner", "joint venture",
]

company_variants = [
    "nvidia",
    "nvda",
    "geforce",
    "cuda",
    "nvidia corp",
    "nvidia corporation",
]

# фильтр ненужных новостей
exclude_patterns = [
    r"\bbenchmark\b",
    r"\breview\b",
    r"\bpreview\b",
    r"\bgaming\b",
    r"\bgame\b",
    r"\bgamer\b",
    r"\besports\b",
    r"\bfps\b",
    r"\b4k\b",
    r"\bperformance\b",
    r"\bgraphics card\b",
    r"\brtx\s*\d{3,4}\b",
    r"\bgpu\b",
    r"\bchip\b",
]


# забирает текст статьи и обрезает до limit
def get_article_text(url, limit=text_limit):
    try:
        headers = {"user-agent": "Mozilla/5.0"}
        response = requests.get(url, timeout=10, headers=headers)
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


# собирает строку запроса в google news по репутации/имиджу
def build_search_query(company_name, start, end):
    base_query = (
        f"{company_name} reputation OR image OR scandal OR praise OR controversy OR award "
        f"OR recognition OR backlash OR viral OR influential"
    )
    date_filters = f" after:{start.date()} before:{end.date()}"
    return (base_query + date_filters).replace(" ", "+")


# присваивает уровень важности (important / medium / low)
def classify_importance(title, snippet):
    full = f"{title.lower()} {snippet.lower()}"

    strong_negative = [
        "scandal", "controversy", "backlash", "outcry",
        "harassment", "discrimination", "misconduct",
        "toxic workplace", "whistleblower", "data breach",
        "breach", "hacked", "cyberattack", "exposed data",
        "resignation", "steps down", "public apology",
    ]

    strong_positive = [
        "award", "prestigious", "top ranking", "best employer",
        "recognized", "global recognition", "won", "acclaimed",
    ]

    medium_patterns = [
        "viral", "buzz", "trending", "praised",
        "public reaction", "ceo", "jensen huang",
    ]

    if any(k in full for k in strong_negative):
        return "important"

    if any(k in full for k in strong_positive):
        return "important"

    if any(k in full for k in medium_patterns):
        return "medium"

    return "low"


# проверяет, относится ли новость к type-5 (reputation/image)
def is_type5_news(title, snippet, importance):
    full = f"{title.lower()} {snippet.lower()}"

    # важные берём всегда
    if importance == "important":
        return True

    # medium берём только если это не типы 1–4
    if importance == "medium":
        if any(w in full for w in finance_shadow):
            return False
        if any(w in full for w in product_shadow):
            return False
        if any(w in full for w in mna_shadow):
            return False
        if any(w in full for w in legal_shadow):
            return False
        return True

    # low не берём
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

        # базовая защита от мусора по ключам
        full = f"{title.lower()} {snippet.lower()}"
        for pattern in exclude_patterns:
            if re.search(pattern, full):
                break
        else:
            importance = classify_importance(title, snippet)
            if is_type5_news(title, snippet, importance):
                rows.append({
                    "date": getattr(entry, "published", None),
                    "title": title,
                    "url": link,
                    "snippet": snippet,
                    "importance": importance,
                })

    return rows


# приводит заголовок к нормализованному виду для сравнения
def clean_title(title):
    t = re.sub(r"[^a-z0-9 ]", " ", title.lower())
    t = re.sub(r"\b(nvidia|nvda|geforce|cuda)\b", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# дедупликация похожих новостей внутри одного дня
def deduplicate_by_day(df, threshold=0.75):
    df = df.copy()
    df["day"] = df["date"].astype(str).str.extract(r"(\d{2}\s\w+\s\d{4})")
    df["t_clean"] = df["title"].apply(clean_title)

    final_idxs = []

    for _, group in df.groupby("day"):
        idxs = list(group.index)
        titles = group["t_clean"].tolist()
        used = set()

        for i in range(len(titles)):
            if idxs[i] in used:
                continue

            base = titles[i]

            for j in range(i + 1, len(titles)):
                if idxs[j] in used:
                    continue

                sim = SequenceMatcher(None, base, titles[j]).ratio()
                if sim > threshold:
                    used.add(idxs[j])

            final_idxs.append(idxs[i])

    df = df.loc[final_idxs].drop(columns=["t_clean", "day"]).reset_index(drop=True)
    return df


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


# возвращает df с reputation / image новостями за последние 3 года
def run_reputation_last_3_years():
    all_rows = []
    months = get_last_3_year_months()

    for year, month in months:
        ranges = split_month(year, month, parts=12)
        for start, end in ranges:
            all_rows.extend(fetch_chunk(start, end))

    if not all_rows:
        return pd.DataFrame(
            columns=["date", "title", "url", "snippet", "importance", "category"]
        )

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["title", "url"]).reset_index(drop=True)
    df = deduplicate_by_day(df)

    df["category"] = "reputation"

    return df


if __name__ == "__main__":
    df_reputation = run_reputation_last_3_years()