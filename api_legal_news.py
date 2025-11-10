import re
from datetime import datetime, timedelta

import feedparser
import pandas as pd
import requests
from bs4 import BeautifulSoup

company = "nvidia"
text_limit = 800

company_variants = [
    "nvidia",
    "nvda",
    "geforce",
    "cuda",
    "nvidia corp",
    "nvidia corporation",
]

# юридические / регуляторные ключевые слова
legal_keywords = [
    # судебные разбирательства / иски
    "lawsuit", "litigation", "class action", "complaint",
    "sues", "sued", "sue", "legal action", "settlement",

    # штрафы / санкции
    "fine", "fined", "penalty", "civil penalty",
    "sanction", "sanctions", "blacklist", "blacklisted",

    # экспорт / ограничения
    "export control", "export controls",
    "export ban", "export bans",
    "export restriction", "export restrictions",
    "ban", "banned", "blocked",

    # антимонопольное / конкуренция
    "antitrust", "competition case", "monopoly",
    "abuse of dominance",

    # расследования / уголовка
    "investigation", "investigations", "probe", "probed",
    "indictment", "charges", "criminal case",
    "fraud", "smuggling", "smuggled", "smuggle",

    # регуляторы / госорганы / лицензии / субсидии
    "sec", "securities and exchange commission",
    "ftc", "federal trade commission",
    "doj", "department of justice",
    "cfius", "european commission", "eu commission",
    "competition authority", "regulator", "regulatory",
    "ministry", "ministry of", "government",
    "license", "licence", "licensing",
    "granted license", "license revoked",
    "subsidy", "subsidies", "grant", "state aid",

    # судебные решения
    "court", "judge", "ruling", "verdict",
    "injunction", "hearing", "trial",
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


def get_article_text(url, limit=text_limit):
    #забирает текст статьи и обрезает до limit
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


def split_month(year, month, parts=12):
    #делит месяц на parts отрезков дат
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


def build_search_query(company_name, start, end):
    #собирает строку запроса в google news с юр/рег-ключами и датами
    base_query = (
        f'{company_name} lawsuit OR litigation OR sanctions OR "export control" OR '
        f'"export ban" OR investigation OR probe OR antitrust OR regulator OR license'
    )
    date_filters = f" after:{start.date()} before:{end.date()}"
    return (base_query + date_filters).replace(" ", "+")


def is_legal_news(title, snippet):
    # проверяет, относится ли новость к юр/рег темам по nvidia
    full = f"{title.lower()} {snippet.lower()}"

    if not any(name in full for name in company_variants):
        return False

    for pattern in exclude_patterns:
        if re.search(pattern, full):
            return False

    legal_hits = sum(keyword in full for keyword in legal_keywords)

    if legal_hits >= 2:
        return True

    if legal_hits == 1:
        if any(trigger in full for trigger in [
            "sanction", "sanctions",
            "export control", "export ban",
            "sec", "ftc", "doj", "department of justice",
            "court", "lawsuit", "investigation", "probe",
        ]):
            return True

    return False


def fetch_chunk(start, end):
    #забирает и фильтрует новости за указанный период
    query = build_search_query(company, start, end)
    url = f"https://news.google.com/rss/search?q={query}&hl=en-us&gl=us&ceid=us:en"

    feed = feedparser.parse(url)
    rows = []

    for entry in feed.entries:
        title = entry.title
        link = entry.link
        snippet = get_article_text(link)

        if is_legal_news(title, snippet):
            rows.append({
                "date": getattr(entry, "published", None),
                "title": title,
                "url": link,
                "snippet": snippet,
            })

    return rows


def get_last_3_year_months():
    #возвращает список (year, month) за последние 3 года до текущего месяца включительно
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


def run_legal_last_3_years():
    #возвращает df с legal-новостями за последние 3 года
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

    df["category"] = "regulatory_legal"
    return df


if __name__ == "__main__":
    df_legal = run_legal_last_3_years()
