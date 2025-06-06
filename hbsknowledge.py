import logging
from pathlib import Path
from datetime import datetime
from pyairtable import Api
from playwright.sync_api import sync_playwright

# === Airtable config ===
AIRTABLE_API_KEY = "your_airtable_api_key"  # Replace or use os.environ["AIRTABLE_API_KEY"]
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tblpMPs5RCoP0PmFt"

FIELDS = {
    "title": "fldL68m7PxHr8Yu07",
    "date": "fldI5VF5zXon5VFso",
    "author": "fldY18cLWUknYKnFF",
    "summary": "fld7l8QViOEKRmqKt",
    "url": "flduwlWuezNKWsEDb",
    "image": "fldv4pJxM5npkieFJ",
    "category": "fldlf7UamHsrgCEKb",
}

# === Logging setup ===
log_path = Path("hbs_scrape_log.txt")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)

# === Allowed categories ===
allowed_categories = {
    # Add full list from earlier
    "Strategy and Innovation", "Finance and Investing", "Leadership", "Marketing", "Entrepreneurship",
    "Operations", "Corporate Strategy", "Business Strategy", "Innovation Strategy", "Change Management",
    "Data and Technology", "Management", "Behavioral Finance", "Technology Adoption", "Customer Satisfaction",
    "Human Resources", "Diversity and Inclusion", "Sustainability", "Decision Making",
    # Add rest as needed
}

def get_valid_categories(raw_categories):
    valid = [cat for cat in raw_categories if cat in allowed_categories]
    return valid or ["Strategy and Innovation"]

def run_scraper():
    logging.info("🚀 Starting HBS scraper with Playwright...")

    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, TABLE_ID)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        url = "https://www.library.hbs.edu/working-knowledge/collections/strategy-and-innovation"
        page.goto(url, timeout=60000)
        page.wait_for_selector(".hbs-tease-feed__item", timeout=10000)
        logging.info("✅ Page loaded and articles detected")

        articles = page.query_selector_all(".hbs-tease-feed__item")
        logging.info(f"🔍 Found {len(articles)} articles")

        for i, article in enumerate(articles, 1):
            try:
                title = article.query_selector("h2 a").inner_text().strip()
                article_url = article.query_selector("h2 a").get_attribute("href")
                if not article_url.startswith("http"):
                    article_url = "https://www.library.hbs.edu" + article_url

                img = article.query_selector("figure img")
                image_url = img.get_attribute("src") if img else ""

                summary_elem = article.query_selector(".hbs-article-tease__teaser")
                summary = summary_elem.inner_text().strip() if summary_elem else ""

                author_elem = article.query_selector(".hbs-byline__author span")
                author = author_elem.inner_text().strip() if author_elem else ""

                time_elem = article.query_selector("time")
                date_str = time_elem.get_attribute("datetime") if time_elem else None
                publication_date = date_str if date_str else datetime.today().strftime("%Y-%m-%d")

                raw_categories = [a.inner_text().strip() for a in article.query_selector_all(".hbs-article-tease__overline a")]
                valid_categories = get_valid_categories(raw_categories)

                record = {
                    FIELDS["title"]: title,
                    FIELDS["date"]: publication_date,
                    FIELDS["author"]: author,
                    FIELDS["summary"]: summary,
                    FIELDS["url"]: article_url,
                    FIELDS["image"]: image_url,
                    FIELDS["category"]: valid_categories,
                }

                table.create(record)
                logging.info(f"✅ Uploaded article {i}: {title}")

            except Exception as e:
                logging.error(f"❌ Error processing article {i}: {e}")

        browser.close()
        logging.info("🏁 Scraping completed successfully.")

if __name__ == "__main__":
    run_scraper()
