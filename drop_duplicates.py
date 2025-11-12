import pandas as pd
import re

MIN_COMMON = 4

STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "if", "on", "in", "at", "for", "to",
    "of", "by", "with", "about", "from", "up", "out", "over", "after", "before",
    "between", "under", "into", "through", "during", "without", "within",
    "against", "among",
}

TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def normalize_title(title):
    if pd.isna(title):
        return set()
    words = TOKEN_RE.findall(str(title).lower())
    clean = []
    for w in words:
        if w in STOPWORDS:
            continue
        if len(w) < 3:
            continue
        if w.isdigit():
            continue
        clean.append(w)
    return set(clean)


def is_duplicate(current_set, kept_sets):
    for s in kept_sets:
        if len(current_set & s) >= MIN_COMMON:
            return True
    return False


def dedupe_one_day(day_df):
    kept_indices = []
    kept_sets = []

    for idx, row in day_df.iterrows():
        word_set = normalize_title(row["title"])

        if not word_set:
            kept_indices.append(idx)
            kept_sets.append(word_set)
            continue

        if is_duplicate(word_set, kept_sets):
            continue

        kept_indices.append(idx)
        kept_sets.append(word_set)

    return day_df.loc[kept_indices]


def dedupe_news_df(df):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["date"])
    df["day"] = df["date"].dt.date
    df = df.sort_values("date")

    parts = [dedupe_one_day(g) for _, g in df.groupby("day")]
    result = pd.concat(parts).drop(columns=["day"])
    return result

list_files = ['nvidia_deals_2023_2025_oct','nvidia_regulatory_2023_to_2025_oct','nvidia_competitor_success_2023_2025_oct','nvidia_products_2023jan_2025oct',
              'nvidia_controversies_2023_2025_oct','nvidia_financial_2023_2025_oct']
for INPUT_FILE in list_files:
    OUTPUT_FILE = INPUT_FILE + '_drop_duplicates'
    df = pd.read_csv(INPUT_FILE + '.csv')
    result = dedupe_news_df(df)
    result.to_csv(OUTPUT_FILE + '.csv', index=False)
    OUTPUT_FILE = INPUT_FILE + '_drop_duplicates'
    print(f"Исходных строк: {len(df)}")
    print(f"После удаления дублей: {len(result)}")
    print(f"Сохранено в: {OUTPUT_FILE}")