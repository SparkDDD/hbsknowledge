import os
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pyairtable import Api

# --- Setup logging ---
logging.basicConfig(
    filename="hbs_scrape_log.txt",
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logging.info("🚀 Starting HBS scraper with Playwright...")

# --- Airtable config ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tblpMPs5RCoP0PmFt"

FIELDS = {
    "title": "fldL68m7PxHr8Yu07",
    "date": "fldI5VF5zXon5VFso",
    "author": "fldY18cLWUknYKnFF",
    "summary": "fld7l8QViOEKRmqKt",
    "url": "flduwlWuezNKWsEDb",
    "image": "fldv4pJxM5npkieFJ",
    "category": "fldlf7UamHsrgCEKb"
}

api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, TABLE_ID)

URL = "https://www.library.hbs.edu/working-knowledge/collections/strategy-and-innovation"

# --- Playwright start ---
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    try:
        page.goto(URL, timeout=60000)
        page.wait_for_selector("li.hbs-tease-feed__item", timeout=15000)
        logging.info("✅ Page loaded and articles detected")
    except Exception as e:
        logging.error(f"❌ Failed to load page or content: {e}")
        browser.close()
        exit(1)

    html = page.content()
    browser.close()

# --- Parse HTML ---
soup = BeautifulSoup(html, "html.parser")
articles = soup.select("li.hbs-tease-feed__item")
logging.info(f"🔍 Found {len(articles)} articles")

if not articles:
    logging.warning("⚠️ No articles found. Exiting.")
    exit(0)

# --- Extract & upload each article ---
for idx, item in enumerate(articles, 1):
    try:
        title_elem = item.select_one("h2.hbs-article-tease__title a")
        title = title_elem.get_text(strip=True)
        article_url = title_elem["href"]

        teaser_elem = item.select_one("p.hbs-article-tease__teaser")
        summary = teaser_elem.get_text(strip=True) if teaser_elem else ""

        img_elem = item.select_one("figure img")
        image_url = img_elem["src"] if img_elem else ""

        author_elem = item.select_one(".hbs-byline__author span")
        author = author_elem.get_text(strip=True) if author_elem else ""

        time_elem = item.select_one("time")
        try:
            pub_date = datetime.strptime(time_elem.get_text(strip=True), "%B %d, %Y").date().isoformat()
        except:
            pub_date = ""

        category_elem = item.select_one(".hbs-article-tease__overline a")
        category = category_elem.get_text(strip=True) if category_elem else "Strategy and Innovation"

        record = {
            FIELDS["title"]: title,
            FIELDS["date"]: pub_date,
            FIELDS["author"]: author,
            FIELDS["summary"]: summary,
            FIELDS["url"]: article_url,
            FIELDS["image"]: image_url,
            FIELDS["category"]: category,
        }

        table.create(record)
        logging.info(f"✅ Uploaded article {idx}: {title}")

    except Exception as e:
        logging.error(f"❌ Error processing article {idx}: {e}")

logging.info("🏁 Scraping completed successfully.")
