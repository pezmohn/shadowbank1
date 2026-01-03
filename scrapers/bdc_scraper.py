"""BDC Scraper for SEC 10-K filings."""

import logging
import sys
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from sec_edgar_downloader import Downloader

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

# Ares Capital Corp CIK
ARES_CIK = "0001287750"
DOWNLOAD_DIR = Path(__file__).parent.parent / "data" / "sec_filings"


def download_latest_10k():
    """Download the latest 10-K filing for Ares Capital Corp.

    Returns:
        Path to the downloaded filing directory, or None on failure.
    """
    try:
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        dl = Downloader("ShadowBank", "risk@shadowbank.local", DOWNLOAD_DIR)
        dl.get("10-K", ARES_CIK, limit=1)

        # Find the downloaded filing
        ares_dir = DOWNLOAD_DIR / "sec-edgar-filings" / ARES_CIK / "10-K"
        if ares_dir.exists():
            filings = list(ares_dir.iterdir())
            if filings:
                logger.info(f"Downloaded 10-K filing to {filings[0]}")
                return filings[0]

        logger.warning("No 10-K filing found after download")
        return None

    except Exception as e:
        logger.error(f"Failed to download 10-K: {e}")
        return None


def parse_schedule_of_investments(filing_path):
    """Parse the Consolidated Schedule of Investments from a 10-K filing.

    Args:
        filing_path: Path to the filing directory.

    Returns:
        List of loan dictionaries, or empty list on failure.

    Note:
        This is a placeholder for complex HTML table parsing.
        Use mock_parse_filing() for MVP testing.
    """
    try:
        # Find the primary document (usually .htm or .html)
        html_files = list(filing_path.glob("*.htm")) + list(filing_path.glob("*.html"))

        if not html_files:
            logger.warning("No HTML files found in filing")
            return []

        # Read and parse the filing
        with open(html_files[0], "r", encoding="utf-8", errors="ignore") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        # Look for Schedule of Investments section
        # This is simplified - real parsing would need more sophisticated logic
        schedule_markers = [
            "Consolidated Schedule of Investments",
            "Schedule of Investments",
            "SCHEDULE OF INVESTMENTS"
        ]

        for marker in schedule_markers:
            element = soup.find(string=lambda t: t and marker in t)
            if element:
                logger.info(f"Found schedule marker: {marker}")
                # TODO: Implement actual table parsing logic
                # For now, return empty and use mock data
                break

        logger.info("Schedule parsing not yet implemented - use mock_parse_filing()")
        return []

    except Exception as e:
        logger.error(f"Failed to parse filing: {e}")
        return []


def mock_parse_filing():
    """Generate mock BDC loan data for MVP testing.

    Returns:
        List of 5 realistic loan dictionaries.
    """
    today = datetime.now().strftime("%Y-%m-%d")

    mock_loans = [
        {
            "borrower": "Apex Software Solutions LLC",
            "fund": "Ares",
            "sector": "Technology",
            "cost": 15_500_000.00,
            "fair_value": 15_200_000.00,
            "date_added": today
        },
        {
            "borrower": "Midwest Healthcare Partners",
            "fund": "Ares",
            "sector": "Healthcare",
            "cost": 22_000_000.00,
            "fair_value": 21_750_000.00,
            "date_added": today
        },
        {
            "borrower": "Continental Manufacturing Inc",
            "fund": "Ares",
            "sector": "Industrials",
            "cost": 8_750_000.00,
            "fair_value": 8_400_000.00,
            "date_added": today
        },
        {
            "borrower": "Summit Business Services Corp",
            "fund": "Ares",
            "sector": "Business Services",
            "cost": 12_300_000.00,
            "fair_value": 12_300_000.00,
            "date_added": today
        },
        {
            "borrower": "Pacific Retail Holdings LLC",
            "fund": "Ares",
            "sector": "Consumer Retail",
            "cost": 6_800_000.00,
            "fair_value": 5_950_000.00,
            "date_added": today
        }
    ]

    logger.info(f"Generated {len(mock_loans)} mock loan records")
    return mock_loans


def run_scraper(use_mock=True):
    """Run the BDC scraper pipeline.

    Args:
        use_mock: If True, use mock data instead of parsing real filings.

    Returns:
        Number of records saved.
    """
    try:
        logger.info("Starting BDC scraper run")

        # Initialize database
        init_db()

        if use_mock:
            loans = mock_parse_filing()
        else:
            # Download and parse real filing
            filing_path = download_latest_10k()
            if filing_path:
                loans = parse_schedule_of_investments(filing_path)
            else:
                logger.warning("No filing downloaded, falling back to mock data")
                loans = mock_parse_filing()

        # Save loans to database
        saved_count = 0
        for loan in loans:
            try:
                save_loan(loan)
                saved_count += 1
                logger.info(f"Saved loan: {loan['borrower']}")
            except Exception as e:
                logger.error(f"Failed to save loan {loan.get('borrower')}: {e}")

        logger.info(f"BDC scraper completed: {saved_count}/{len(loans)} records saved")
        return saved_count

    except Exception as e:
        logger.error(f"BDC scraper failed: {e}")
        return 0


if __name__ == "__main__":
    print("Running BDC Scraper (MVP mode with mock data)...")
    count = run_scraper(use_mock=True)
    print(f"Saved {count} loan records to database")
    print(f"Log file: {LOG_PATH}")
