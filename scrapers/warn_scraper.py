"""WARN Notice Scraper for layoff tracking."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from io import StringIO

import pandas as pd
import requests
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
# NEW YORK STATE WARN SCRAPER - Year-Specific Archive
# =============================================================================

def get_ny_warn_url(year):
    """Generate the NY WARN archive URL for a specific year.

    Args:
        year: The year to generate the URL for.

    Returns:
        URL string for the year-specific WARN notices page.
    """
    return f"https://dol.ny.gov/{year}-warn-notices"


def fetch_ny_warn_page(url):
    """Fetch the NY WARN page.

    Args:
        url: URL to fetch.

    Returns:
        Tuple of (HTML content, status_code) or (None, status_code) on failure.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.text, 200
        else:
            return None, response.status_code
    except requests.RequestException as e:
        logger.warning(f"Request failed for {url}: {e}")
        return None, 0


def fetch_ny_warn_with_fallback():
    """Fetch NY WARN data with year fallback mechanism.

    Tries current year first, falls back to previous year, then main page.

    Returns:
        Tuple of (HTML content, year used) or (None, None) on failure.
    """
    current_year = datetime.now().year
    previous_year = current_year - 1

    # URLs to try in order
    urls_to_try = [
        (get_ny_warn_url(current_year), current_year),
        (get_ny_warn_url(previous_year), previous_year),
        ("https://dol.ny.gov/warn-notices", current_year),  # Main page fallback
    ]

    for url, year in urls_to_try:
        logger.info(f"Trying URL: {url}")
        html_content, status_code = fetch_ny_warn_page(url)

        if html_content:
            logger.info(f"Successfully fetched WARN data from {url}")
            return html_content, year

        logger.info(f"URL not available (status: {status_code})")

    logger.error("Failed to fetch WARN data from all sources")
    return None, None


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
    """Scrape WARN notices from New York State Department of Labor archive.

    Uses year-specific archive pages with fallback mechanism.
    Returns ALL records (no date filtering) to populate dashboard with historical data.

    Returns:
        List of WARN notice dictionaries matching the database schema:
        - company: Company name
        - state: "NY"
        - employees: Number of affected employees (0 if not available)
        - date_filed: Date in YYYY-MM-DD format
    """
    notices = []

    try:
        # Fetch with fallback mechanism
        html_content, year_used = fetch_ny_warn_with_fallback()

        if not html_content:
            logger.warning("No WARN data available from NY DOL")
            return notices

        # Use pandas to extract tables from HTML
        tables = pd.read_html(StringIO(html_content), flavor="lxml")

        if not tables:
            logger.warning("No tables found on NY WARN page")
            return notices

        # The WARN table is typically the first/main table
        df = tables[0]
        logger.info(f"Found table with {len(df)} rows and columns: {list(df.columns)}")

        # Column mapping for year-specific archive pages
        # Common columns: 'Company', 'Date Posted'/'Notice Date', 'Number Affected'/'Workforce Affected', 'Reason'
        column_mapping = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if "company" in col_lower or "employer" in col_lower or "name" in col_lower:
                column_mapping[col] = "Company"
            elif "date" in col_lower and ("posted" in col_lower or "notice" in col_lower):
                column_mapping[col] = "Date"
            elif "date" in col_lower and "Date" not in column_mapping.values():
                # Fallback: any date column
                column_mapping[col] = "Date"
            elif "affected" in col_lower or "employee" in col_lower or "worker" in col_lower or "number" in col_lower:
                column_mapping[col] = "Employees"
            elif "reason" in col_lower or "type" in col_lower:
                column_mapping[col] = "Reason"

        df = df.rename(columns=column_mapping)
        logger.info(f"Mapped columns: {column_mapping}")

        # Ensure required columns exist
        if "Company" not in df.columns and len(df.columns) > 0:
            df = df.rename(columns={df.columns[0]: "Company"})

        if "Date" not in df.columns:
            for col in df.columns:
                if "date" in str(col).lower():
                    df = df.rename(columns={col: "Date"})
                    break

        # Process ALL records (no date filtering)
        for idx, row in df.iterrows():
            try:
                # Use column indexing to avoid Series ambiguity
                company = ""
                if "Company" in df.columns:
                    val = row["Company"]
                    company = str(val.iloc[0] if hasattr(val, 'iloc') else val).strip()

                if not company or company.lower() == "nan":
                    continue

                date_val = None
                if "Date" in df.columns:
                    val = row["Date"]
                    date_val = val.iloc[0] if hasattr(val, 'iloc') else val

                date_filed = parse_date(date_val)
                if not date_filed:
                    # Use a placeholder date if parsing fails
                    date_filed = f"{year_used}-01-01"

                emp_val = 0
                if "Employees" in df.columns:
                    val = row["Employees"]
                    emp_val = val.iloc[0] if hasattr(val, 'iloc') else val

                employees = parse_employees(emp_val)

                notice = {
                    "company": company,
                    "state": "NY",
                    "employees": employees,
                    "date_filed": date_filed
                }
                notices.append(notice)

            except Exception as e:
                logger.warning(f"Failed to parse row {idx}: {e}")
                continue

        logger.info(f"Scraped {len(notices)} NY WARN notices from {year_used} archive")

    except requests.RequestException as e:
        logger.error(f"Failed to fetch NY WARN page: {e}")
    except ValueError as e:
        logger.error(f"No tables found on NY WARN page: {e}")
    except Exception as e:
        logger.error(f"Failed to parse NY WARN page: {e}")

    return notices


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
    print("Running WARN Scraper (NY State - Year Archive)...")
    count = run_scraper()
    print(f"Saved {count} WARN notices to database")
    print(f"Log file: {LOG_PATH}")
