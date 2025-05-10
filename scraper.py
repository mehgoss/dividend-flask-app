import nest_asyncio
import asyncio
from playwright.async_api import async_playwright
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm
import re
import yfinance as yf
import pandas as pd
import time
import logging
from functools import lru_cache
from googlesearch import search
import random

nest_asyncio.apply()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sanitize_filename(filename):
    filename = re.sub(r'^https?://[^/]+/', '', filename)
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    filename = filename.strip('. ').replace('..', '.')
    return filename if filename else 'unnamed_article'

async def scrape_current_month_dividends():
    current_year = datetime.now().year
    current_month = datetime.now().month
    html_file = "dividends_page_current_month.html"

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            logger.info("Navigating to dividends page...")
            await page.goto("https://blogs.easyequities.co.za/topic/dividends-update", timeout=60000)

            with open(html_file, "w", encoding="utf-8") as file:
                articles_found = True
                with tqdm(desc="Loading current month articles", unit="page") as pbar:
                    while articles_found:
                        try:
                            date_elements = await page.query_selector_all('div.post-date')
                            articles_found = False

                            for date_element in date_elements:
                                date_text = await date_element.inner_text()
                                try:
                                    for fmt in ["%B %d, %Y", "%d %B %Y", "%Y-%m-%d"]:
                                        try:
                                            article_date = datetime.strptime(date_text.strip(), fmt)
                                            break
                                        except ValueError:
                                            continue
                                    else:
                                        logger.warning(f"Could not parse date: {date_text}")
                                        continue

                                    logger.info(f"Found article dated: {date_text}")
                                    if article_date.year == current_year and article_date.month == current_month:
                                        articles_found = True
                                    else:
                                        articles_found = False
                                        break
                                except Exception as e:
                                    logger.error(f"Error parsing date '{date_text}': {e}")
                                    continue

                            content = await page.content()
                            file.write(content)

                            if articles_found:
                                load_more_button = await page.query_selector('a#loadMore')
                                if load_more_button:
                                    await page.wait_for_selector('a#loadMore', state='visible', timeout=60000)
                                    await load_more_button.click()
                                    await page.wait_for_timeout(6000)
                                    pbar.update(1)
                                else:
                                    logger.info("No more articles to load")
                                    break
                            else:
                                logger.info("No more articles from current month")
                                break
                        except Exception as e:
                            logger.error(f"Error during scraping: {e}")
                            break
            await browser.close()
            return html_file
        except Exception as e:
            logger.error(f"Failed to initialize Playwright browser: {e}")
            raise

def process_dividend_data(html_file):
    try:
        with open(html_file, "r", encoding="utf-8") as file:
            soup = BeautifulSoup(file, 'html.parser')
    except FileNotFoundError:
        logger.error(f"HTML file {html_file} not found.")
        return {}

    articles = soup.find_all('a', class_='LinkBox')
    logger.info(f"Found {len(articles)} articles in {html_file}")

    article_data = []
    Data = {}

    def truncate_name(name, max_length=30):
        if len(name) > max_length:
            return name[:max_length]
        return name

    def clean_instrument_name(name):
        name = name.strip()
        if len(name) > 100 or any(keyword in name.lower() for keyword in ['tariff', 'investor sentiment', 'bear market']):
            return None
        name = re.sub(r'\b(Limited|Corporation|Incorporated|PLC|SE|Group)\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s+', ' ', name).strip()
        return name if name else None

    for article in articles:
        title = article.text.strip()
        link = article.get('href', '')
        if link.startswith('https://blogs.easyequities.co.za/'):
            article_data.append({
                'title': truncate_name(str(link).replace('https://blogs.easyequities.co.za/','')),
                'link': link
            })
        else:
            logger.info(f"Skipping non-EasyEquities link: {link}")

    for article in tqdm(article_data, desc="Scraping articles", unit="article"):
        Data[article['title']] = {}
        if article['link']:
            try:
                response = requests.get(article['link'])
                soup = BeautifulSoup(response.content, 'html.parser')
                content = soup.find_all('p')
                Stff = ''
                for y in content:
                    Stff += y.text.strip() + '\n'
                    text = y.text.strip()
                    if 'per share' in text.lower():
                        Fn = text.split('will be paying ')
                        if len(Fn) > 1:
                            instrument = clean_instrument_name(Fn[0])
                            if instrument:
                                Data[article['title']][instrument] = {"Dividends": str(Fn[-1]).replace('per share.', "")}
                    elif 'dividend' in text.lower():
                        match = re.search(r'(\w[\w\s]+?)\s+(?:dividend|pays|declares)\s+.*?([\d.]+)\s*(ZAR|USD|EUR|$|€|cents|pence)?', text, re.IGNORECASE)
                        if match:
                            instrument = clean_instrument_name(match.group(1))
                            if instrument:
                                dividend = match.group(2)
                                currency = match.group(3) or ''
                                Data[article['title']][instrument] = {"Dividends": f"{dividend} {currency}".strip()}
                logger.info(f"Processed article: {article['title']} - {len(Data[article['title']])} dividend entries")
            except Exception as e:
                logger.error(f"Error scraping {article['title']}: {e}")
        safe_filename = sanitize_filename(article['title'])
        with open(f"{safe_filename}.txt", 'w', encoding='utf-8') as f:
            f.write(Stff)

    logger.info(f'Done scraping {len([n for n in Data.keys()])} sites')
    return Data

@lru_cache(maxsize=100)
def get_yfinance_price(symbol):
    for attempt in range(7):
        try:
            ticker = yf.Ticker(symbol)
            price = ticker.history(period="1d")['Close'].iloc[-1]
            return f"{price:.2f}", "yfinance"
        except Exception as e:
            if "Too Many Requests" in str(e):
                delay = 5 * (2 ** attempt)
                logger.warning(f"Rate limit hit for {symbol}, retrying ({attempt+1}/7) after {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Error fetching yfinance price for {symbol}: {e}")
                return "0.00", "yfinance"
    logger.error(f"Failed to fetch price for {symbol} after 7 attempts.")
    return "0.00", "yfinance"

@lru_cache(maxsize=100)
def get_yfinance_region(symbol):
    for attempt in range(7):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            currency = info.get('currency', '').upper()
            exchange = info.get('exchange', '').upper()
            if currency == 'USD' or exchange in ['NYQ', 'NAS', 'AMX']:
                return 'USA', "yfinance"
            elif currency == 'EUR' or exchange in ['FRA', 'PAR', 'AMS', 'MCE']:
                return 'EUR', "yfinance"
            else:
                return 'Unknown', "yfinance"
        except Exception as e:
            if "Too Many Requests" in str(e):
                delay = 5 * (2 ** attempt)
                logger.warning(f"Rate limit hit for {symbol}, retrying ({attempt+1}/7) after {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Error determining region for {symbol}: {e}")
                return 'Unknown', "yfinance"
    logger.error(f"Failed to determine region for {symbol} after 7 attempts.")
    return 'Unknown', "yfinance"

def google_search_instrument(instrument_name):
    symbol_map = {
        'PSG Financial Services': ('PSG.JO', 'SA', 'Manual Mapping'),
        'PSGFINANCIALSERVICESLTD': ('PSG.JO', 'SA', 'Manual Mapping')
    }
    if instrument_name in symbol_map:
        return symbol_map[instrument_name]

    for attempt in range(3):
        try:
            query = f"{instrument_name} stock symbol"
            for url in search(query, num_results=3):
                time.sleep(random.uniform(2, 5))
                try:
                    response = requests.get(url, timeout=10)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    if 'jse.co.za' in url:
                        symbol_tag = soup.find('div', class_='field--name-field-alpha-code')
                        if symbol_tag:
                            symbol = symbol_tag.find('span').text.strip()
                            return symbol, 'SA', 'JSE'
                    title = soup.find('title').text.lower() if soup.find('title') else ''
                    if 'bloomberg' in url or 'reuters' in url or 'finance.yahoo.com' in url:
                        symbol_match = re.search(r'\b([A-Z0-9]+(\.JO|\.L|\.DE|\.PA|\.AS)?)\b', title)
                        if symbol_match:
                            symbol = symbol_match.group(1)
                            if symbol.endswith('.JO'):
                                return symbol, 'SA', 'Financial Site'
                            elif symbol.endswith(('.L', '.DE', '.PA', '.AS')):
                                region = 'EUR' if symbol.endswith(('.DE', '.PA', '.AS')) else 'USA'
                                return symbol, region, 'Financial Site'
                            else:
                                region, _ = get_yfinance_region(symbol)
                                return symbol, region, 'Financial Site'
                except Exception as e:
                    logger.warning(f"Error processing URL {url} for {instrument_name}: {e}")
                    continue
            return None, 'Unknown', 'Google Search'
        except Exception as e:
            logger.error(f"Google search error for {instrument_name} (attempt {attempt+1}/3): {e}")
            time.sleep(10 * (2 ** attempt))
    logger.error(f"Failed to search Google for {instrument_name} after 3 attempts.")
    return None, 'Unknown', 'Google Search'

def google_finance_price(symbol):
    for attempt in range(3):
        try:
            query = f"{symbol} stock price site:finance.google.com"
            for url in search(query, num_results=1):
                time.sleep(random.uniform(2, 5))
                if "finance.google.com" in url:
                    response = requests.get(url)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    price_tag = soup.find('div', class_='YMlKec fxKbKc')
                    if price_tag:
                        price = price_tag.text.strip().replace('$', '').replace('€', '')
                        return f"{float(price):.2f}", "Google Finance"
            return "0.00", "Google Finance"
        except Exception as e:
            logger.error(f"Google Finance price error for {symbol} (attempt {attempt+1}/3): {e}")
            time.sleep(10 * (2 ** attempt))
    logger.error(f"Failed to fetch Google Finance price for {symbol} after 3 attempts.")
    return "0.00", "Google Finance"

def save_to_csv(dividend_data):
    csv_data = []
    unknown_instruments = []
    BASE_URL = "https://www.jse.co.za"
    WORD_REPLACEMENTS = {"Property": "Prop", "Funding": "Fund", "Limited": "Ltd"}

    def get_jse_price(instrument_url):
        try:
            response = requests.get(instrument_url)
            soup = BeautifulSoup(response.text, "html.parser")
            price_tag = soup.find("div", class_="instrument-delta__price")
            return price_tag.text.replace("Price", "").strip(), "JSE"
        except Exception as e:
            logger.error(f"Error fetching JSE price: {e}")
            return "0.00", "JSE"

    def search_jse_instrument(name):
        symbol_map = {
            'PSG Financial Services': 'PSG.JO',
            'PSGFINANCIALSERVICESLTD': 'PSG.JO'
        }
        if name in symbol_map:
            return {
                "Instrument": name,
                "Symbol": symbol_map[name],
                "Price": get_jse_price(f"{BASE_URL}/instruments/{symbol_map[name].replace('.', '')}")[0],
                "Link": f"{BASE_URL}/instruments/{symbol_map[name].replace('.', '')}",
                "Source": "Manual Mapping"
            }

        search_queries = [name]
        modified_name = name
        for original, replacement in WORD_REPLACEMENTS.items():
            modified_name = modified_name.replace(original, replacement)
        modified_name = modified_name.replace("eft", "")
        search_queries.append(modified_name)
        first_word = name.split()[:2]
        search_queries.append(" ".join(first_word))

        for query in search_queries:
            search_url = f"{BASE_URL}/search?keys={query}"
            try:
                response = requests.get(search_url)
                soup = BeautifulSoup(response.text, "html.parser")
                search_results = soup.find_all("div", class_="search-result search-result--instrument")
                instruments = []

                for result in search_results:
                    link_tag = result.find("a")
                    instrument_name = link_tag.text.strip() if link_tag else "N/A"
                    instrument_link = f"{BASE_URL}{link_tag['href']}" if link_tag else "N/A"
                    symbol_tag = result.find("div", class_="field--name-field-alpha-code")
                    symbol = symbol_tag.find("span").text.strip() if symbol_tag else "N/A"
                    price, source = get_jse_price(instrument_link) if instrument_link != "N/A" else ("0.00", "JSE")
                    instruments.append({
                        "Instrument": instrument_name,
                        "Symbol": symbol,
                        "Price": price,
                        "Link": instrument_link,
                        "Source": source
                    })

                if instruments:
                    return instruments[0]
            except Exception as e:
                logger.error(f"Error searching JSE for {query}: {e}")
        return {"Instrument": name, "Symbol": "N/A", "Price": "0.00", "Link": "N/A", "Source": "JSE"}

    for article in dividend_data:
        for instrument, details in dividend_data[article].items():
            dividend = details.get("Dividends", "N/A")
            jse_result = search_jse_instrument(instrument)
            if jse_result["Price"] != "0.00" and jse_result["Symbol"] != "N/A":
                csv_data.append({
                    "Region": "SA",
                    "Instrument": jse_result["Instrument"],
                    "Symbol": jse_result["Symbol"],
                    "Dividend": dividend,
                    "Price": jse_result["Price"],
                    "Article": article,
                    "Source": jse_result["Source"]
                })
            else:
                google_symbol, region, source = google_search_instrument(instrument)
                if google_symbol:
                    symbol = google_symbol
                    price, price_source = google_finance_price(symbol)
                    csv_data.append({
                        "Region": region,
                        "Instrument": instrument,
                        "Symbol": symbol,
                        "Dividend": dividend,
                        "Price": price,
                        "Article": article,
                        "Source": price_source
                    })
                else:
                    symbol = instrument.replace(" ", "").upper()
                    region, region_source = get_yfinance_region(symbol)
                    price, price_source = get_yfinance_price(symbol)
                    csv_data.append({
                        "Region": region,
                        "Instrument": instrument,
                        "Symbol": symbol,
                        "Dividend": dividend,
                        "Price": price,
                        "Article": article,
                        "Source": price_source
                    })
                    if region == "Unknown":
                        unknown_instruments.append(f"{instrument} ({symbol})")

    df = pd.DataFrame(csv_data)
    df = df.sort_values(by=["Region", "Instrument"])

    csv_file = os.path.join('static', 'data', 'dividends_with_prices_current_month.csv')
    df.to_csv(csv_file, index=False, encoding='utf-8')
    logger.info(f"Saved dividend data to {csv_file}")
    if unknown_instruments:
        logger.info(f"Instruments in Unknown section: {', '.join(set(unknown_instruments))}")

def scrape_and_process_dividends():
    html_file = asyncio.run(scrape_current_month_dividends())
    dividend_data = process_dividend_data(html_file)
    if dividend_data:
        save_to_csv(dividend_data)
    else:
        logger.warning("No dividend data scraped")
