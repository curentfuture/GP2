# -*- coding: utf-8 -*-

import json

config = {
    "logging_enabled": True,
    "log_level": "INFO",
    "log_file": "parser.log"
}

with open("logging_config.json", "w", encoding="utf-8") as f:
    json.dump(config, f, indent=4, ensure_ascii=False)

print("Файл logging_config.json создан")

import logging
import json
import os

def setup_logging(config_path="logging_config.json"):
    """Настройка логов через файл"""
    if not os.path.exists(config_path):
        logging.basicConfig(level=logging.INFO)
        return

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    if not config.get("logging_enabled", True):
        logging.basicConfig(level=logging.CRITICAL)
        return

    level_name = config.get("log_level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    log_file = config.get("log_file", "parser.log")

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

setup_logging()
logger = logging.getLogger(__name__)
# -*- coding: utf-8 -*-
"""Ещё одна копия блокнота "Парсинг с ноута.ipynb"
    !apt-get update
!apt-get install -y chromium-chromedriver chromium-browser
!cp /usr/lib/chromium-browser/chromedriver /usr/bin
!pip install selenium beautifulsoup4 pandas openpyxl lxml requests
"""


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup

# === ЛОГИРОВАНИЕ ===
import logging
import json
import os

def setup_logging(config_path="logging_config.json"):
    """Настройка логов через внешний файл."""
    if not os.path.exists(config_path):
        # если файла конфига нет — простой лог в консоль
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
        return

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # можно выключить логирование через конфиг
    if not config.get("logging_enabled", True):
        logging.basicConfig(
            level=logging.CRITICAL,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
        return

    level_name = config.get("log_level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    log_file = config.get("log_file", "parser.log")

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

setup_logging()
logger = logging.getLogger(__name__)

"""# БЛОК 2
настройка браузера
"""

def setup_browser():
    logger.info("Инициализация headless браузера (Chrome)")

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--remote-debugging-port=9222')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36')
    chrome_options.binary_location = '/usr/bin/chromium-browser'

    try:
        driver = webdriver.Chrome(options=chrome_options)
        logger.info("Браузер успешно запущен")
        return driver
    except Exception as e:
        logger.critical(f"Не удалось запустить браузер: {e}", exc_info=True)
        raise

"""# БЛОК 3
Прокрутка страниц
"""

def load_all_articles(driver, target_articles=10100):

    logger.info(f"Открываю страницу поиска CNBC, цель: {target_articles} статей")

    driver.get("https://www.cnbc.com/search/?query=nvidia&qsearchterm=nvidia")
    time.sleep(10)

    newest_button = driver.find_element(By.CSS_SELECTOR, "#sortdate")
    newest_button.click()

    last_count = 0
    scroll_attempts = 0
    max_scrolls = target_articles * 2
    no_change_count = 0

    logger.info("Начинаю скроллинг ленты результатов")

    while scroll_attempts < max_scrolls and no_change_count < 20:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)

        current_articles = driver.find_elements(By.CSS_SELECTOR, '.SearchResult-searchResultContent')
        current_count = len(current_articles)
        scroll_attempts += 1

        logger.debug(f"Прокрутка {scroll_attempts}: найдено {current_count} статей")

        if current_count == last_count:
            no_change_count += 1
            logger.debug(f"Количество статей не изменилось ({no_change_count}/20)")
        else:
            no_change_count = 0

        last_count = current_count

        if current_count >= target_articles:
            logger.info(f"Достигнута цель: {target_articles} статей")
            break

    logger.info(f"Загрузка завершена. Всего статей: {last_count}")
    return last_count

"""# БЛОК 4
Захват данных со страницы
"""

def parse_search_page(driver):

    logger.info("Начинаю парсинг карточек статей со страницы поиска")
    articles_data = []

    time.sleep(3)

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    search_container = soup.find('div', id='searchcontainer')
    if not search_container:
        logger.warning("searchcontainer не найден — возвращаю пустой список")
        return articles_data

    article_cards = search_container.find_all('div', class_='SearchResult-searchResult')
    logger.info(f"Найдено {len(article_cards)} карточек статей")

    for i, card in enumerate(article_cards):
        try:
            # заголовок
            title = "Не найден"
            title_container = card.find('div', class_='SearchResult-searchResultTitle')
            if title_container:
                link_element = title_container.find('a')
                if link_element:
                    title = link_element.get_text(strip=True, separator=' ')

                    url = "Не найдена"
                    if link_element.get('href'):
                        url = link_element['href']
                        if url.startswith('/'):
                            url = "https://www.cnbc.com" + url

            if title == "Не найден":
                logger.debug(f"Карточка {i+1}: заголовок не найден — пропуск")
                continue

            # подзаголовок
            preview = "Не найдено"
            preview_element = card.find('p', class_='SearchResult-searchResultPreview')
            if preview_element:
                preview = preview_element.get_text(strip=True, separator=' ')

            author = "Не указан"
            author_element = card.find('a', class_='SearchResult-author')
            if author_element:
                author = author_element.get_text(strip=True)

            publish_date = "Не указана"
            date_element = card.find('span', class_='SearchResult-publishedDate')
            if date_element:
                publish_date = date_element.get_text(strip=True)

            # раздел
            topic = "Не указана"
            topic_element = card.find('div', class_='SearchResult-searchResultEyebrow')
            if topic_element:
                topic = topic_element.get_text(strip=True)

            articles_data.append({
                'topic': topic,
                'title': title,
                'preview': preview,
                'author': author,
                'publish_date': publish_date,
                'url': url
            })

        except Exception as e:
            logger.error(f"Ошибка при обработке карточки {i+1}: {e}", exc_info=True)
            continue

    logger.info(f"Парсинг карточек завершен. Всего уникальных статей: {len(articles_data)}")
    return articles_data

"""# БЛОК 5
Парсинг текста статей
"""

def parse_article_details(url):
    logger.debug(f"Парсю текст статьи: {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        full_content = "Текст не найден"
        content_elements = soup.find_all('div', class_=['ArticleBody-articleBody', 'group'])
        if not content_elements:
            content_elements = soup.find_all('div', attrs={'data-testid': 'article-body'})

        if content_elements:
            paragraphs = []
            for content_element in content_elements:
                paragraph_elements = content_element.find_all('p')
                for p in paragraph_elements:
                    text = p.get_text(strip=True, separator=' ')
                    if text and len(text) > 20:
                        paragraphs.append(text)

            if paragraphs:
                full_content = ' '.join(paragraphs)

        else:
            content_elements = soup.find("div", class_="ClipPlayer-clipPlayerIntro")
            if content_elements:
                content_element = content_elements.find(
                    'div',
                    class_=["ClipPlayer-clipPlayerIntroSummary", "ClipPlayer-investingClubClipPlayerIntroSummary"]
                )
                if content_element:
                    text = content_element.get_text(strip=True, separator=' ')
                    full_content = text

        if full_content == "Текст не найден":
            logger.warning(f"Не удалось извлечь текст статьи: {url}")

        return full_content

    except Exception as e:
        logger.error(f"Ошибка при обработке статьи {url}: {e}", exc_info=True)
        return "Текст не найден"

"""# БЛОК 6
Основной процесс парсинга
"""

def main_parsing_process():
    logger.info("=== Старт основного процесса парсинга ===")
    driver = setup_browser()

    try:
        total_elements = load_all_articles(driver, 10100)
        logger.info(f"После прокрутки найдено ~{total_elements} элементов")

        articles_data = parse_search_page(driver)

        if not articles_data:
            logger.warning("Не найдено релевантных статей — выход")
            print("Не найдено релевантных статей")
            return None

        final_data = []

        for i, article in enumerate(articles_data):

            details = parse_article_details(article['url'])

            complete_article = {
                'publish_date': article['publish_date'],
                'title': article['title'],
                'content': details,
                'author': article['author'],
                'topic': article['topic'],
                'preview': article['preview'],
                'url': article['url']
            }

            final_data.append(complete_article)
            time.sleep(1)

            if (i + 1) % 500 == 0:
                logger.info(f"Обработано {i+1} статей, сохраняю временный файл")
                temp_df = pd.DataFrame(final_data)
                temp_filename = 'nvidia_news_progress_' + str(i+1) + '.csv'
                temp_df.to_csv(temp_filename, index=False)

        if final_data:
            df = pd.DataFrame(final_data)

            csv_filename = 'nvidia_news_final.csv'
            df.to_csv(csv_filename, index=False)

            excel_filename = 'nvidia_news_final.xlsx'
            df.to_excel(excel_filename, index=False)

            logger.info("Конец парсинга")
            logger.info(f"Всего новостей: {len(df)}")
            logger.info(f"CSV сохранён: {csv_filename}")
            logger.info(f"Excel сохранён: {excel_filename}")

            print("Конец парсинга")
            print("Всего: ", len(df), "новостей")
            print("CSV: ", csv_filename)
            print("Excel: ", excel_filename)

            return df

        logger.warning("final_data пустой, нечего сохранять")
        return None

    except Exception as e:
        logger.critical(f"Критическая ошибка в main_parsing_process: {e}", exc_info=True)
        print("Критическая ошибка: ", e)
        import traceback
        traceback.print_exc()
        return None
    finally:
        try:
            driver.quit()
            logger.info("Браузер закрыт")
        except Exception as e:
            logger.warning(f"Ошибка при закрытии браузера: {e}", exc_info=True)
        print("Браузер закрыт")

"""# БЛОК 7
Финальный датасет
"""

logger.info("Запускаю main_parsing_process()")
final_dataset = main_parsing_process()

def show_results(dataset):

    if dataset is not None:
        logger.info("Печатаю краткую статистику по датасету")
        print("Размер: ", dataset.shape[0], "строк", dataset.shape[1], "колонок")
        print("Дата: ", len(dataset[dataset['publish_date'] != 'Не указана'])/len(dataset))
        print("Автор: ", len(dataset[dataset['author'] != 'Не указан'])/len(dataset))
        print("Текст: ", len(dataset[dataset['content'] != 'Текст не найден'])/len(dataset))
        print("Тема: ", len(dataset[dataset['topic'] != 'Не указана'])/len(dataset))
        print("Превью: ", len(dataset[dataset['preview'] != 'Не найдено'])/len(dataset))

if final_dataset is not None:
    show_results(final_dataset)

def parse_complex_date(date_str):

    format = '%m/%d/%Y %I:%M:%S %p'
    try:
        return datetime.strptime(date_str, format)
    except ValueError:
        logger.debug(f"Не удалось распарсить дату: {date_str}")
        return None

if final_dataset is not None:
    final_dataset['publish_date'] = final_dataset['publish_date'].apply(parse_complex_date)
    logger.info("Дата публикации преобразована к datetime (где удалось)")

final_dataset

#оригинальный парсер
# -*- coding: utf-8 -*-
"""Ещё одна копия блокнота "Парсинг с ноута.ipynb"

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1j8ADs9k3t_o4CxuFoqnE8lbyFS-UAU5V


!apt-get update
!apt-get install -y chromium-chromedriver chromium-browser
!cp /usr/lib/chromium-browser/chromedriver /usr/bin
!pip install selenium beautifulsoup4 pandas openpyxl lxml requests
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup

"""# БЛОК 2
настройка браузера
"""

def setup_browser():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--remote-debugging-port=9222')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36')
    chrome_options.binary_location = '/usr/bin/chromium-browser'

    driver = webdriver.Chrome(options=chrome_options)
    return driver

"""# БЛОК 3
Прокрутка страниц
"""

def load_all_articles(driver, target_articles=10100):

    driver.get("https://www.cnbc.com/search/?query=nvidia&qsearchterm=nvidia")
    time.sleep(10)

    newest_button = driver.find_element(By.CSS_SELECTOR, "#sortdate")
    newest_button.click()

    last_count = 0
    scroll_attempts = 0
    max_scrolls = target_articles*2
    no_change_count = 0

    print("Цель: ", target_articles, "статей")

    while scroll_attempts < max_scrolls and no_change_count < 20:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)

        current_articles = driver.find_elements(By.CSS_SELECTOR, '.SearchResult-searchResultContent')
        current_count = len(current_articles)
        scroll_attempts += 1

        print("Прокрутка ", scroll_attempts," : ", current_count, " статей")

        if current_count == last_count:
            no_change_count += 1
            print("Количество не изменилось", no_change_count, "/10")
        else:
            no_change_count = 0

        last_count = current_count


        if current_count >= target_articles:
            print("Достигнута цель в ", target_articles, "статей")
            break

    print("Загрузка завершена. всего статей: ", last_count)
    return last_count

"""# БЛОК 4
Захват данных со страницы
"""

def parse_search_page(driver):

    articles_data = []

    time.sleep(3)

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')


    search_container = soup.find('div', id='searchcontainer')
    if not search_container:
        print("searchcontainer не найден ")
        return articles_data


    article_cards = search_container.find_all('div', class_='SearchResult-searchResult')
    print("Найдено ", len(article_cards), "карточек статей")

    for i, card in enumerate(article_cards):
        try:
            #заголовок
            title = "Не найден"
            title_container = card.find('div', class_='SearchResult-searchResultTitle')
            if title_container:
                link_element = title_container.find('a')
                if link_element:
                    title = link_element.get_text(strip=True, separator = ' ')

                    url = "Не найдена"
                    if link_element.get('href'):
                        url = link_element['href']
                        if url.startswith('/'):
                            url = "https://www.cnbc.com"+url

            if title == "Не найден":
                continue
            #подзаголовок
            preview = "Не найдено"
            preview_element = card.find('p', class_='SearchResult-searchResultPreview')
            if preview_element:
                preview = preview_element.get_text(strip=True, separator = ' ')

            author = "Не указан"
            author_element = card.find('a', class_='SearchResult-author')
            if author_element:
                author = author_element.get_text(strip=True)

            publish_date = "Не указана"
            date_element = card.find('span', class_='SearchResult-publishedDate')
            if date_element:
                publish_date = date_element.get_text(strip=True)
            #раздел
            topic = "Не указана"
            topic_element = card.find('div', class_='SearchResult-searchResultEyebrow')
            if topic_element:
                topic = topic_element.get_text(strip=True)

            articles_data.append({
                'topic': topic,
                'title': title,
                'preview': preview,
                'author': author,
                'publish_date': publish_date,
                'url': url
            })

        except Exception as e:
            print("Ошибка при обработке карточки ", i + 1, e)
            continue

    print("Парсинг завершен. ", len(articles_data), " уникальных статей")
    return articles_data

"""# БЛОК 5
Парсинг текста статей


"""

def parse_article_details(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        full_content = "Текст не найден"
        content_elements = soup.find_all('div', class_=['ArticleBody-articleBody', 'group'])
        if not content_elements:
            content_elements = soup.find_all('div', attrs={'data-testid': 'article-body'})


        if content_elements:
            paragraphs = []
            for content_element in content_elements:
                paragraph_elements = content_element.find_all('p')
                for p in paragraph_elements:
                    text = p.get_text(strip=True, separator = ' ')
                    if text and len(text) > 20:
                        paragraphs.append(text)

            if paragraphs:
                full_content = ' '.join(paragraphs)

        else:
            content_elements = soup.find("div", class_= "ClipPlayer-clipPlayerIntro")
            content_element = content_elements.find('div', class_=["ClipPlayer-clipPlayerIntroSummary", "ClipPlayer-investingClubClipPlayerIntroSummary"])
            text = content_element.get_text(strip=True, separator = ' ')
            full_content = text

        return full_content

    except Exception as e:
        print("Ошибка при обработке статьи", url,":", e)
        return "Текст не найден"

"""# БЛОК 6
Основной процесс парсинга
"""

def main_parsing_process():
    driver = setup_browser()

    try:
        total_elements = load_all_articles(driver, 10100)

        articles_data = parse_search_page(driver)

        if not articles_data:
            print("Не найдено релевантных статей")
            return None

        final_data = []

        for i, article in enumerate(articles_data):

            details = parse_article_details(
                article['url']
            )

            complete_article = {
                'publish_date': article['publish_date'],
                'title': article['title'],
                'content': details,
                'author': article['author'],
                'topic': article['topic'],
                'preview': article['preview'],
                'url': article['url']
            }

            final_data.append(complete_article)
            time.sleep(1)

            if (i + 1) % 500 == 0:
                print("Обработано: ", i + 1, "статей")
                temp_df = pd.DataFrame(final_data)
                temp_filename = 'nvidia_news_progress_' + str(i+1) + '.csv'
                temp_df.to_csv(temp_filename, index=False)

        if final_data:
            df = pd.DataFrame(final_data)

            csv_filename = 'nvidia_news_final.csv'
            df.to_csv(csv_filename, index=False)

            excel_filename = 'nvidia_news_final.xlsx'
            df.to_excel(excel_filename, index=False)

            print("Конец парсинга")

            print("Всего: ", len(df),"новостей")
            print("CSV: ", csv_filename)
            print("Excel: ", excel_filename)

            return df

    except Exception as e:
        print("Критическая ошибка: ", e)
        import traceback
        traceback.print_exc()
        return None
    finally:
        driver.quit()
        print("Браузер закрыт")

"""# БЛОК 7
Финальный датасет
"""

final_dataset = main_parsing_process()

def show_results(dataset):

    if dataset is not None:
        print("Размер: ", dataset.shape[0], "строк", dataset.shape[1], "колонок")
        print("Дата: ",len(dataset[dataset['publish_date'] != 'Не указана'])/len(dataset))
        print("Автор: ", len(dataset[dataset['author'] != 'Не указан'])/len(dataset))
        print("Текст: ", len(dataset[dataset['content'] != 'Текст не найден'])/len(dataset))
        print("Тема: ", len(dataset[dataset['topic'] != 'Не указана'])/len(dataset))
        print("Превью: ", len(dataset[dataset['preview'] != 'Не найдено'])/len(dataset))

if final_dataset is not None:
    show_results(final_dataset)

def parse_complex_date(date_str):

    format = '%m/%d/%Y %I:%M:%S %p'
    try:
        return datetime.strptime(date_str, format)
    except ValueError:
        return None

final_dataset['publish_date'] = final_dataset['publish_date'].apply(parse_complex_date)

final_dataset