import os
import logging
import requests
from datetime import datetime
from pyairtable import Api

# Airtable Configuration
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tblpMPs5RCoP0PmFt"
FIELD_ID_TITLE = "fldL68m7PxHr8Yu07"
FIELD_ID_DATE = "fldI5VF5zXon5VFso"
FIELD_ID_AUTHOR = "fldY18cLWUknYKnFF"
FIELD_ID_SUMMARY = "fld7l8QViOEKRmqKt"
FIELD_ID_URL = "flduwlWuezNKWsEDb"
FIELD_ID_IMAGE = "fldv4pJxM5npkieFJ"
FIELD_ID_CATEGORY = "fldlf7UamHsrgCEKb"
FIELD_ID_OBJECT_ID = "fldohE77B6Wk5raeZ"
FIELD_ID_NEW_CATEGORY = "fldsZRoSMa0eCyiQ6"
FIELD_ID_FACULTY = "fldIWmGyXv9J32RU9"
FIELD_ID_TIMESTAMP = "fldxAyDuvrsAIOBDO"

# Allowed categories (but preserve unlisted ones too)
all_valid_categories = {
    "Accounting", "Advertising", "Asset Management", "Balanced Scorecard",
    "Banks and Banking", "Behavioral Finance", "Borrowing and Debt",
    "Brands and Branding", "Business and Government Relations",
    "Business Cycles", "Business Education", "Business History",
    "Business Model", "Business or Company Management", "Business Startups",
    "Business Strategy", "Capital Markets", "Career and Workplace",
    "Change Management", "Communication", "Communication Strategy",
    "Compensation and Benefits", "Competency and Skills", "Competition",
    "Competitive Advantage", "Competitive Strategy", "Consumer Behavior",
    "Contracts", "Corporate Finance", "Corporate Governance",
    "Corporate Social Responsibility and Impact", "Corporate Strategy",
    "Cost Management", "COVID-19", "Creativity", "Crime and Corruption",
    "Crisis Management", "Customer Relationship Management",
    "Customer Satisfaction", "Data and Technology", "Decision Making",
    "Demand and Consumers", "Design", "Developing Countries and Economies",
    "Development Economics", "Disruption", "Disruptive Innovation",
    "Distribution", "Distribution Channels", "Diversity",
    "Diversity and Inclusion", "Economic Growth", "Economic Systems",
    "Economics", "Economics and Global Commerce", "Emerging Markets",
    "Emotions", "Employee Relationship Management", "Employees", "Employment",
    "Entrepreneurship", "Environmental Accounting",
    "Environmental Sustainability", "Equality and Inequality", "Ethics",
    "Executive Compensation", "Failure", "Family Business", "Finance",
    "Finance and Investing", "Financial Crisis", "Financial Instruments",
    "Financial Markets", "Financing and Loans", "Forecasting and Prediction",
    "Foreign Direct Investment", "Game Theory", "Gender", "Geopolitical Units",
    "Giving and Philanthropy", "Global Strategy", "Globalization",
    "Going Public", "Goodwill Accounting", "Governance",
    "Government and Politics", "Groups and Teams",
    "Growth and Development Strategy", "Happiness", "Health", "History",
    "Human Capital", "Human Resources", "Immigration",
    "Inflation and Deflation", "Information Management",
    "Information Technology", "Infrastructure", "Innovation and Invention",
    "Innovation Strategy", "Insolvency and Bankruptcy",
    "Intellectual Property", "Internet", "Interpersonal Communication",
    "Investment", "Investment Banking", "Investment Portfolio",
    "Jobs and Positions", "Knowledge", "Knowledge Management", "Labor", "Law",
    "Leadership", "Leadership Development", "Leadership Style",
    "Leading Change", "Leveraged Buyouts", "Logistics", "Luxury",
    "Macroeconomics", "Management",
    "Management Analysis, Tools, and Techniques",
    "Management Practices and Processes", "Management Skills",
    "Management Style", "Management Teams", "Managing the Business",
    "Marketing", "Marketing and Consumers", "Marketing Communications",
    "Marketing Strategy", "Markets", "Mergers and Acquisitions",
    "Microeconomics", "Monopoly", "Motivation and Incentives",
    "National Security", "Natural Environment", "Negotiation",
    "Negotiation and Decision Making", "Nonprofit Organizations",
    "Online Advertising", "Operations", "Organizational Change and Adaptation",
    "Organizational Culture", "Organizational Design",
    "Organizational Structure", "Organizations", "Ownership", "Patents",
    "Perception", "Performance", "Performance Evaluation",
    "Performance Improvement", "Performance Productivity",
    "Personal Characteristics", "Personal Development and Career",
    "Personal Finance", "Planning", "Policy", "Political Elections",
    "Power and Influence", "Prejudice and Bias", "Price", "Private Equity",
    "Problems and Challenges", "Product", "Product Design",
    "Product Development", "Product Marketing", "Profit", "Project Finance",
    "Psychology and Behavior", "Race", "Recruitment",
    "Regulation and Compliance", "Relationships", "Research",
    "Research and Development", "Retention", "Risk Management", "Sales",
    "Saving", "Science", "Selection and Staffing", "Service Delivery",
    "Service Operations", "Small Business", "Social Enterprise",
    "Social Entrepreneurship", "Social Issues", "Social Marketing",
    "Social Psychology", "Social Responsibility and Sustainability", "Society",
    "Strategy", "Strategy and Innovation", "Success", "Supply Chain",
    "Supply Chain Management", "SWOT Analysis", "Talent and Talent Management",
    "Taxation", "Technological Innovation", "Technology Adoption",
    "Time Management", "Trade", "Training", "Trust", "Urban Development",
    "Values and Beliefs", "Venture Capital", "Voting", "Wages",
    "Wealth and Poverty", "Weather and Climate Change", "Work-Life Balance",
    "Working Conditions", "Not Defined", "Artificial Intelligence",
    "AI at Work"
}

# Logging Setup
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

# Airtable API
api = Api(os.environ["AIRTABLE_API_KEY"])
table = api.table(BASE_ID, TABLE_ID)
logger.info("✅ Airtable table initialized successfully.")


def get_existing_object_ids():
    logger.info("📦 Loading existing object IDs from Airtable...")
    existing_ids = set()
    params = {"fields[]": ["Object ID"]}
    offset = None
    while True:
        if offset:
            params["offset"] = offset
        response = requests.get(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}",
            headers={
                "Authorization": f"Bearer {os.environ['AIRTABLE_API_KEY']}"
            },
            params=params)
        data = response.json()
        for record in data.get("records", []):
            obj_id = record.get("fields", {}).get("Object ID")
            if obj_id:
                existing_ids.add(obj_id)
        offset = data.get("offset")
        if not offset:
            break
    logger.info(f"📦 Loaded {len(existing_ids)} existing IDs.")
    return existing_ids


def normalize_categories(topics):
    if not topics:
        return ["Not Defined"], []
    cleaned = [t for t in topics if isinstance(t, str)]
    allowed = [t for t in cleaned if t in all_valid_categories]
    new = [t for t in cleaned if t not in all_valid_categories]
    return allowed or ["Not Defined"], new


def upload_article(hit, existing_ids):
    obj_id = hit.get("id")
    if obj_id in existing_ids:
        logger.info(f"⏩ Skipping existing article: {hit.get('title')}")
        return

    url = hit.get("url", "")
    date_str = hit.get("sortDate") or hit.get("display", {}).get("date")
    try:
        date_iso = datetime.fromisoformat(date_str.replace(
            "Z", "")).date().isoformat() if date_str else ""
    except Exception:
        date_iso = ""

    authors = hit.get("author") or []
    if not authors:
        byline = hit.get("display", {}).get("byline", [])
        authors = [item.get("label", "")
                   for item in byline] if isinstance(byline, list) else []
    author_str = ", ".join(authors)

    faculty = hit.get("faculty", [])
    faculty_str = ", ".join(faculty) if isinstance(faculty, list) else ""

    image_url = ""
    thumb = hit.get("display", {}).get("thumbnail", {}).get("src", "")
    if thumb:
        image_url = "https:" + thumb if thumb.startswith("//") else thumb

    allowed_cats, new_cats = normalize_categories(hit.get("topic", []))

    # Get current timestamp in ISO format with timezone
    current_timestamp = datetime.now().isoformat()

    record = {
        FIELD_ID_OBJECT_ID: obj_id,
        FIELD_ID_TITLE: hit.get("title", ""),
        FIELD_ID_DATE: date_iso,
        FIELD_ID_AUTHOR: author_str,
        FIELD_ID_FACULTY: faculty_str,
        FIELD_ID_SUMMARY: hit.get("description", ""),
        FIELD_ID_URL: url,
        FIELD_ID_IMAGE: image_url,
        FIELD_ID_CATEGORY: allowed_cats,
        FIELD_ID_TIMESTAMP: current_timestamp,
    }
    if new_cats:
        record[FIELD_ID_NEW_CATEGORY] = ", ".join(new_cats)

    try:
        table.create(record)
        logger.info(f"✅ Created new record: {hit.get('title')}")
    except Exception as e:
        logger.error(f"❌ Error creating record in Airtable: {e}")


def fetch_and_upload():
    logger.info("🚀 Starting HBS API scraper (checking recent articles)...")
    existing_ids = get_existing_object_ids()

    # We'll check 2 pages of 10 articles each
    page_size = 10
    max_articles = 20
    articles_checked = 0

    for offset in range(0, max_articles, page_size):
        logger.info(f"🌐 Fetching page {offset // page_size + 1}")
        url = f"https://www.library.hbs.edu/api/search/query?from={offset}&size={page_size}&index=modern&facets=industry%2Cfaculty%2Cunit&filters=(subset:working-knowledge+AND+contentType:Article)&sort=sortDate:desc"

        try:
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()
            hits = data.get("hits", [])

            if not hits:
                logger.info("✅ No more articles found.")
                break

            for hit in hits:
                upload_article(hit, existing_ids)
                articles_checked += 1

            if articles_checked >= max_articles:
                logger.info(f"✅ Checked maximum number of articles ({max_articles})")
                break

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching articles: {e}")
            break

    logger.info(f"✅ Finished checking {articles_checked} articles.")


if __name__ == "__main__":
    fetch_and_upload()
