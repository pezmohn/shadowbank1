"""BDC Scraper for SEC 10-Q filings - Distress Signal Analysis."""

import logging
import re
import sys
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from sec_edgar_downloader import Downloader
from tenacity import retry, stop_after_attempt, wait_exponential

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from db_manager import init_db, save_loan

# Configure logging to file
LOG_PATH = Path(__file__).parent.parent / "scraping_log.txt"
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Ares Capital Corp CIK and Ticker
ARES_CIK = "0001287750"
ARES_TICKER = "ARCC"
DOWNLOAD_DIR = Path(__file__).parent.parent / "data" / "sec_filings"

# Distress keywords to search for
DISTRESS_KEYWORDS = [
    "non-accrual",
    "nonaccrual",
    "non accrual",
    "payment default",
    "payment-default",
]


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_latest_10q():
    """Download the latest 10-Q filing for Ares Capital Corp.

    Returns:
        Path to the downloaded filing directory, or None on failure.
    """
    try:
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading latest 10-Q for {ARES_TICKER} (CIK: {ARES_CIK})")
        dl = Downloader("ShadowBank", "risk@shadowbank.local", DOWNLOAD_DIR)
        dl.get("10-Q", ARES_CIK, limit=1)

        # Find the downloaded filing
        ares_dir = DOWNLOAD_DIR / "sec-edgar-filings" / ARES_CIK / "10-Q"
        if ares_dir.exists():
            filings = sorted(ares_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
            if filings:
                logger.info(f"Downloaded 10-Q filing to {filings[0]}")
                return filings[0]

        logger.warning("No 10-Q filing found after download")
        return None

    except Exception as e:
        logger.error(f"Failed to download 10-Q: {e}")
        return None


def count_distress_keywords(filing_path):
    """Count distress keywords in a 10-Q filing.

    Args:
        filing_path: Path to the filing directory.

    Returns:
        Dictionary with keyword counts:
        - non_accrual_count: Count of 'non-accrual' variations
        - payment_default_count: Count of 'payment default' variations
        - total_distress_count: Total of all distress keywords
    """
    counts = {
        "non_accrual_count": 0,
        "payment_default_count": 0,
        "total_distress_count": 0
    }

    try:
        # Find the primary document - check multiple formats
        # SEC filings may come as .htm, .html, or full-submission.txt
        html_files = list(filing_path.glob("*.htm")) + list(filing_path.glob("*.html"))
        txt_files = list(filing_path.glob("*.txt"))

        if html_files:
            # Find the largest HTML file (usually the main filing)
            main_file = max(html_files, key=lambda p: p.stat().st_size)
        elif txt_files:
            # Use the full-submission.txt file
            main_file = max(txt_files, key=lambda p: p.stat().st_size)
        else:
            logger.warning("No filing documents found")
            return counts

        logger.info(f"Parsing filing: {main_file.name} ({main_file.stat().st_size / 1024 / 1024:.1f} MB)")

        # Read and parse the filing
        with open(main_file, "r", encoding="utf-8", errors="ignore") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        # Get all text content
        text = soup.get_text().lower()

        # Count non-accrual variations
        non_accrual_patterns = [
            r"non-accrual",
            r"nonaccrual",
            r"non\s+accrual",
        ]
        for pattern in non_accrual_patterns:
            matches = re.findall(pattern, text)
            counts["non_accrual_count"] += len(matches)

        # Count payment default variations
        payment_default_patterns = [
            r"payment\s+default",
            r"payment-default",
        ]
        for pattern in payment_default_patterns:
            matches = re.findall(pattern, text)
            counts["payment_default_count"] += len(matches)

        counts["total_distress_count"] = counts["non_accrual_count"] + counts["payment_default_count"]

        logger.info(f"Distress keyword counts: non-accrual={counts['non_accrual_count']}, "
                   f"payment-default={counts['payment_default_count']}, "
                   f"total={counts['total_distress_count']}")

        return counts

    except Exception as e:
        logger.error(f"Failed to parse filing: {e}")
        return counts


def create_risk_record(distress_counts):
    """Create a risk record from distress keyword counts.

    Args:
        distress_counts: Dictionary with keyword counts from count_distress_keywords().

    Returns:
        Dictionary matching the database schema for bdc_loans.
    """
    today = datetime.now().strftime("%Y-%m-%d")

    return {
        "borrower": "Ares Portfolio Aggregate",
        "fund": "Ares",
        "sector": "Diversified",
        "cost": 0,  # Placeholder
        "fair_value": distress_counts["non_accrual_count"],  # Distress signal count
        "date_added": today
    }


def run_scraper():
    """Run the BDC scraper pipeline.

    Downloads the latest 10-Q filing for Ares Capital Corp,
    counts distress keywords, and saves a risk record to the database.

    Returns:
        Number of records saved (0 or 1).
    """
    try:
        logger.info("Starting BDC scraper run")

        # Initialize database
        init_db()

        # Download the latest 10-Q filing
        filing_path = download_latest_10q()

        if not filing_path:
            logger.error("Failed to download 10-Q filing")
            return 0

        # Count distress keywords
        distress_counts = count_distress_keywords(filing_path)

        # Create and save the risk record
        risk_record = create_risk_record(distress_counts)

        try:
            save_loan(risk_record)
            logger.info(f"Saved risk record: {risk_record['borrower']} "
                       f"(non-accrual count: {risk_record['fair_value']})")
            return 1
        except Exception as e:
            logger.error(f"Failed to save risk record: {e}")
            return 0

    except Exception as e:
        logger.error(f"BDC scraper failed: {e}")
        return 0


if __name__ == "__main__":
    print("Running BDC Scraper (Ares Capital 10-Q Distress Analysis)...")
    count = run_scraper()
    if count > 0:
        print("Successfully saved Ares risk record to database")
    else:
        print("Failed to save risk record - check scraping_log.txt")
    print(f"Log file: {LOG_PATH}")
