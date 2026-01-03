"""BDC Scraper for SEC 10-Q filings - Trend Signal Analysis."""

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

# Number of quarters to analyze for trend
NUM_QUARTERS = 4


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_10q_filings(limit=4):
    """Download multiple 10-Q filings for Ares Capital Corp.

    Args:
        limit: Number of filings to download.

    Returns:
        List of paths to downloaded filing directories, sorted by date (oldest first).
    """
    try:
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading last {limit} 10-Q filings for {ARES_TICKER} (CIK: {ARES_CIK})")
        dl = Downloader("ShadowBank", "risk@shadowbank.local", DOWNLOAD_DIR)
        dl.get("10-Q", ARES_CIK, limit=limit)

        # Find all downloaded filings
        ares_dir = DOWNLOAD_DIR / "sec-edgar-filings" / ARES_CIK / "10-Q"
        if ares_dir.exists():
            filings = sorted(ares_dir.iterdir(), key=lambda p: p.name)  # Sort by accession number (chronological)
            logger.info(f"Found {len(filings)} 10-Q filings")
            return filings

        logger.warning("No 10-Q filings found after download")
        return []

    except Exception as e:
        logger.error(f"Failed to download 10-Q filings: {e}")
        return []


def extract_filing_date(filing_path):
    """Extract the filing date from an SEC filing.

    Args:
        filing_path: Path to the filing directory.

    Returns:
        Filing date as string (YYYY-MM-DD) or None if not found.
    """
    try:
        # The accession number contains the date: 0001287750-YY-NNNNNN
        # Try to extract from directory name first
        dir_name = filing_path.name
        # Format: 0001287750-25-000046 -> extract year
        parts = dir_name.split("-")
        if len(parts) >= 2:
            year_part = parts[1]
            if len(year_part) == 2:
                year = 2000 + int(year_part)
            else:
                year = int(year_part)

        # Try to find filing date in the document
        html_files = list(filing_path.glob("*.htm")) + list(filing_path.glob("*.html"))
        txt_files = list(filing_path.glob("*.txt"))

        main_file = None
        if html_files:
            main_file = max(html_files, key=lambda p: p.stat().st_size)
        elif txt_files:
            main_file = max(txt_files, key=lambda p: p.stat().st_size)

        if main_file:
            with open(main_file, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            # Look for FILED AS OF DATE or CONFORMED PERIOD OF REPORT
            date_patterns = [
                r"FILED AS OF DATE:\s*(\d{8})",
                r"CONFORMED PERIOD OF REPORT:\s*(\d{8})",
                r"DATE AS OF CHANGE:\s*(\d{8})",
            ]

            for pattern in date_patterns:
                match = re.search(pattern, content)
                if match:
                    date_str = match.group(1)
                    # Format: YYYYMMDD
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        # Fallback: use accession number to estimate quarter
        # This is approximate but better than nothing
        if 'year' in dir():
            # Estimate quarter from accession sequence
            seq = int(parts[2]) if len(parts) >= 3 else 1
            if seq < 20:
                quarter_month = "03"
            elif seq < 35:
                quarter_month = "06"
            elif seq < 50:
                quarter_month = "09"
            else:
                quarter_month = "12"
            return f"{year}-{quarter_month}-15"

        return None

    except Exception as e:
        logger.warning(f"Failed to extract filing date: {e}")
        return None


def count_distress_keywords(filing_path):
    """Count distress keywords in a 10-Q filing.

    Args:
        filing_path: Path to the filing directory.

    Returns:
        Dictionary with keyword counts and filing info.
    """
    counts = {
        "non_accrual_count": 0,
        "payment_default_count": 0,
        "total_distress_count": 0,
        "filing_date": None,
        "filing_id": filing_path.name
    }

    try:
        # Extract filing date
        counts["filing_date"] = extract_filing_date(filing_path)

        # Find the primary document
        html_files = list(filing_path.glob("*.htm")) + list(filing_path.glob("*.html"))
        txt_files = list(filing_path.glob("*.txt"))

        if html_files:
            main_file = max(html_files, key=lambda p: p.stat().st_size)
        elif txt_files:
            main_file = max(txt_files, key=lambda p: p.stat().st_size)
        else:
            logger.warning(f"No filing documents found in {filing_path}")
            return counts

        logger.info(f"Parsing filing: {filing_path.name} ({main_file.stat().st_size / 1024 / 1024:.1f} MB)")

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

        logger.info(f"Filing {filing_path.name}: non-accrual={counts['non_accrual_count']}, "
                   f"date={counts['filing_date']}")

        return counts

    except Exception as e:
        logger.error(f"Failed to parse filing {filing_path}: {e}")
        return counts


def determine_trend_signal(quarterly_data):
    """Determine the trend signal from quarterly data.

    Args:
        quarterly_data: List of dicts with non_accrual_count, sorted chronologically.

    Returns:
        Tuple of (signal_str, trend_description).
    """
    if len(quarterly_data) < 2:
        return "INSUFFICIENT_DATA", "Not enough quarters to determine trend"

    counts = [q["non_accrual_count"] for q in quarterly_data]

    # Calculate trend
    first_half_avg = sum(counts[:len(counts)//2]) / max(1, len(counts)//2)
    second_half_avg = sum(counts[len(counts)//2:]) / max(1, len(counts) - len(counts)//2)

    # Also check latest vs earliest
    change = counts[-1] - counts[0]
    pct_change = (change / max(1, counts[0])) * 100 if counts[0] > 0 else 0

    if second_half_avg > first_half_avg * 1.1 or change > 10:
        signal = "DETERIORATING"
        color = "[!]"
    elif second_half_avg < first_half_avg * 0.9 or change < -10:
        signal = "IMPROVING"
        color = "[+]"
    else:
        signal = "STABLE"
        color = "[=]"

    return signal, color


def create_risk_record(filing_data, quarter_label):
    """Create a risk record from filing analysis.

    Args:
        filing_data: Dictionary with keyword counts and filing date.
        quarter_label: Label for this quarter (e.g., "Q1 2024").

    Returns:
        Dictionary matching the database schema for bdc_loans.
    """
    filing_date = filing_data.get("filing_date") or datetime.now().strftime("%Y-%m-%d")

    return {
        "borrower": f"Ares Portfolio ({quarter_label})",
        "fund": "Ares Capital",
        "sector": "Diversified",
        "cost": 0,
        "fair_value": filing_data["non_accrual_count"],
        "date_added": filing_date
    }


def run_scraper():
    """Run the BDC scraper pipeline with trend analysis.

    Downloads the last 4 10-Q filings for Ares Capital Corp,
    analyzes distress trends, and saves records to the database.

    Returns:
        Number of records saved.
    """
    try:
        logger.info("Starting BDC scraper run (Trend Analysis)")

        # Initialize database
        init_db()

        # Download last 4 10-Q filings
        filing_paths = download_10q_filings(limit=NUM_QUARTERS)

        if not filing_paths:
            logger.error("Failed to download any 10-Q filings")
            return 0

        # Analyze each filing
        quarterly_data = []
        for filing_path in filing_paths:
            data = count_distress_keywords(filing_path)
            quarterly_data.append(data)

        # Sort by filing date (oldest first)
        quarterly_data.sort(key=lambda x: x.get("filing_date") or "")

        # Determine trend signal
        signal, color = determine_trend_signal(quarterly_data)

        # Build trend string for output
        trend_parts = []
        for i, q in enumerate(quarterly_data):
            quarter_num = i + 1
            count = q["non_accrual_count"]
            date = q.get("filing_date", "Unknown")
            trend_parts.append(f"Q{quarter_num}({count})")

        trend_str = " -> ".join(trend_parts)

        # Save records to database
        saved_count = 0
        for i, data in enumerate(quarterly_data):
            quarter_label = f"Q{i+1}"
            if data.get("filing_date"):
                # Extract year and quarter from date
                try:
                    dt = datetime.strptime(data["filing_date"], "%Y-%m-%d")
                    q_num = (dt.month - 1) // 3 + 1
                    quarter_label = f"Q{q_num} {dt.year}"
                except:
                    pass

            record = create_risk_record(data, quarter_label)

            try:
                save_loan(record)
                saved_count += 1
                logger.info(f"Saved: {record['borrower']} - Risk Score: {record['fair_value']}")
            except Exception as e:
                logger.error(f"Failed to save record: {e}")

        # Log summary
        logger.info(f"Trend Analysis: {trend_str}. Signal: {signal}")
        logger.info(f"BDC scraper completed: {saved_count}/{len(quarterly_data)} records saved")

        # Print summary to console
        print(f"\n{'='*60}")
        print(f"ARES CAPITAL DISTRESS TREND ANALYSIS")
        print(f"{'='*60}")
        print(f"Trend: {trend_str}")
        print(f"Signal: {color} {signal}")
        print(f"{'='*60}")
        print(f"Records saved: {saved_count}")

        return saved_count

    except Exception as e:
        logger.error(f"BDC scraper failed: {e}")
        return 0


if __name__ == "__main__":
    print("Running BDC Scraper (Ares Capital 10-Q Trend Analysis)...")
    count = run_scraper()
    if count > 0:
        print(f"\nSuccessfully saved {count} quarterly risk records to database")
    else:
        print("Failed to save risk records - check scraping_log.txt")
    print(f"Log file: {LOG_PATH}")
