"""WARN Notice Scraper for layoff tracking."""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from db_manager import init_db, save_warn

# Configure logging to file
LOG_PATH = Path(__file__).parent.parent / "scraping_log.txt"
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================================
# NEW YORK STATE WARN SCRAPER
# =============================================================================

NY_WARN_URL = "https://dol.ny.gov/warn-notices"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_ny_warn_page():
    """Fetch the NY WARN page with retry logic.

    Returns:
        HTML content of the page.

    Raises:
        requests.RequestException: If all retries fail.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    response = requests.get(NY_WARN_URL, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def parse_date(date_str):
    """Parse date string to standardized format.

    Args:
        date_str: Date string in various formats.

    Returns:
        Date in YYYY-MM-DD format or None if parsing fails.
    """
    if pd.isna(date_str) or not date_str:
        return None

    date_str = str(date_str).strip()

    # Try common date formats
    formats = [
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%Y-%m-%d",
        "%B %d, %Y",
        "%b %d, %Y",
        "%m/%d/%y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def parse_employees(emp_str):
    """Parse employee count from string.

    Args:
        emp_str: Employee count string (may contain non-numeric chars).

    Returns:
        Integer employee count or 0 if parsing fails.
    """
    if pd.isna(emp_str):
        return 0

    # Extract digits only
    digits = "".join(filter(str.isdigit, str(emp_str)))
    return int(digits) if digits else 0


def scrape_ny_warn_notices():
    """Scrape WARN notices from New York State Department of Labor.

    Returns:
        List of WARN notice dictionaries matching the database schema:
        - company: Company name
        - state: "NY"
        - employees: Number of affected employees (0 if not available)
        - date_filed: Date in YYYY-MM-DD format
    """
    notices = []

    try:
        logger.info(f"Fetching NY WARN data from {NY_WARN_URL}")
        html_content = fetch_ny_warn_page()

        # Use pandas to extract tables from HTML with StringIO wrapper
        tables = pd.read_html(StringIO(html_content), flavor="lxml")

        if not tables:
            logger.warning("No tables found on NY WARN page")
            return notices

        # The WARN table is typically the first/main table
        df = tables[0]
        logger.info(f"Found table with {len(df)} rows and columns: {list(df.columns)}")

        # NY DOL specific column mapping
        # Columns: ['Company Name', 'Region', 'Date Posted', 'Notice Dated']
        column_mapping = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if "company" in col_lower or "employer" in col_lower:
                column_mapping[col] = "Company"
            elif "date posted" in col_lower or "posted" in col_lower:
                # Use "Date Posted" as the primary date
                column_mapping[col] = "Date"
            elif "employee" in col_lower or "worker" in col_lower or "affected" in col_lower:
                column_mapping[col] = "Employees"
            elif "region" in col_lower:
                column_mapping[col] = "Region"

        df = df.rename(columns=column_mapping)
        logger.info(f"Mapped columns: {column_mapping}")

        # Ensure required columns exist
        if "Company" not in df.columns:
            df = df.rename(columns={df.columns[0]: "Company"})

        if "Date" not in df.columns:
            # Fallback: look for any date-like column
            for col in df.columns:
                if "date" in str(col).lower():
                    df = df.rename(columns={col: "Date"})
                    break

        # Filter for last 7 days
        cutoff_date = datetime.now() - timedelta(days=7)
        logger.info(f"Filtering for notices after {cutoff_date.strftime('%Y-%m-%d')}")

        for _, row in df.iterrows():
            try:
                company = str(row.get("Company", "")).strip()
                if not company or company.lower() == "nan":
                    continue

                date_filed = parse_date(row.get("Date"))
                if not date_filed:
                    continue

                # Check if within last 7 days
                try:
                    notice_date = datetime.strptime(date_filed, "%Y-%m-%d")
                    if notice_date < cutoff_date:
                        continue
                except ValueError:
                    continue

                # NY table may not have employee count - default to 0
                employees = parse_employees(row.get("Employees", 0))

                notice = {
                    "company": company,
                    "state": "NY",
                    "employees": employees,
                    "date_filed": date_filed
                }
                notices.append(notice)

            except Exception as e:
                logger.warning(f"Failed to parse row: {e}")
                continue

        logger.info(f"Scraped {len(notices)} NY WARN notices from last 7 days")

    except requests.RequestException as e:
        logger.error(f"Failed to fetch NY WARN page: {e}")
    except ValueError as e:
        logger.error(f"No tables found on NY WARN page: {e}")
    except Exception as e:
        logger.error(f"Failed to parse NY WARN page: {e}")

    return notices


# State WARN page URLs - for future expansion
STATE_WARN_URLS = {
    "NY": "https://dol.ny.gov/warn-notices",
}


# =============================================================================
# MAIN SCRAPER FUNCTION
# =============================================================================

def scrape_warn_sites():
    """Scrape WARN notices from state websites.

    Currently scrapes New York State. Additional states can be added
    by implementing state-specific scrapers.

    Returns:
        List of WARN notice dictionaries with keys:
        - company: Company name
        - state: Two-letter state code
        - employees: Number of affected employees
        - date_filed: Date the notice was filed
    """
    all_notices = []

    # Scrape New York
    try:
        ny_notices = scrape_ny_warn_notices()
        all_notices.extend(ny_notices)
    except Exception as e:
        # Never crash the main loop - log and continue
        logger.error(f"Error scraping NY: {e}")

    # Add more states here as scrapers are implemented
    # try:
    #     ca_notices = scrape_ca_warn_notices()
    #     all_notices.extend(ca_notices)
    # except Exception as e:
    #     logger.error(f"Error scraping CA: {e}")

    logger.info(f"Scraped {len(all_notices)} total WARN notices")
    return all_notices


def run_scraper():
    """Run the WARN scraper pipeline.

    Returns:
        Number of records saved.
    """
    try:
        logger.info("Starting WARN scraper run")

        # Initialize database
        init_db()

        # Get WARN notices from real sources
        notices = scrape_warn_sites()

        # Save to database
        saved_count = 0
        for notice in notices:
            try:
                save_warn(notice)
                saved_count += 1
                logger.info(f"Saved WARN: {notice['company']} ({notice['state']})")
            except Exception as e:
                logger.error(f"Failed to save WARN {notice.get('company')}: {e}")

        logger.info(f"WARN scraper completed: {saved_count}/{len(notices)} records saved")
        return saved_count

    except Exception as e:
        logger.error(f"WARN scraper failed: {e}")
        return 0


if __name__ == "__main__":
    print("Running WARN Scraper (NY State)...")
    count = run_scraper()
    print(f"Saved {count} WARN notices to database")
    print(f"Log file: {LOG_PATH}")
