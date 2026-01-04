"""BDC Scraper for SEC 10-Q filings - Multi-Fund Trend Analysis."""

import logging
import re
import sys
import time
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

# =============================================================================
# BDC UNIVERSE - Top 5 BDCs by Market Cap/AUM (~$100B+ total assets)
# =============================================================================

BDC_UNIVERSE = [
    {"ticker": "ARCC", "name": "Ares Capital"},
    {"ticker": "OBDC", "name": "Blue Owl Capital"},
    {"ticker": "BXSL", "name": "Blackstone Secured Lending"},
    {"ticker": "FSK",  "name": "FS KKR Capital"},
    {"ticker": "MAIN", "name": "Main Street Capital"},
]

DOWNLOAD_DIR = Path(__file__).parent.parent / "data" / "sec_filings"

# Number of quarters to analyze for trend per BDC
NUM_QUARTERS = 4

# Rate limiting delay between SEC requests (seconds)
SEC_RATE_LIMIT_DELAY = 3


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_10q_filings(ticker, limit=4):
    """Download multiple 10-Q filings for a given ticker.

    Args:
        ticker: Stock ticker symbol (e.g., "ARCC").
        limit: Number of filings to download.

    Returns:
        List of paths to downloaded filing directories, sorted by date (oldest first).
    """
    try:
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading last {limit} 10-Q filings for {ticker}")
        dl = Downloader("ShadowBank", "risk-bot@shadowbank.com", DOWNLOAD_DIR)
        dl.get("10-Q", ticker, limit=limit)

        # Find all downloaded filings - search by ticker
        filings_base = DOWNLOAD_DIR / "sec-edgar-filings"
        if filings_base.exists():
            # Find the CIK directory for this ticker
            for cik_dir in filings_base.iterdir():
                tenq_dir = cik_dir / "10-Q"
                if tenq_dir.exists():
                    filings = sorted(tenq_dir.iterdir(), key=lambda p: p.name)
                    if filings:
                        logger.info(f"Found {len(filings)} 10-Q filings for {ticker}")
                        return filings[-limit:]  # Return most recent 'limit' filings

        logger.warning(f"No 10-Q filings found for {ticker}")
        return []

    except Exception as e:
        logger.error(f"Failed to download 10-Q filings for {ticker}: {e}")
        return []


def extract_filing_date(filing_path):
    """Extract the filing date from an SEC filing.

    Args:
        filing_path: Path to the filing directory.

    Returns:
        Filing date as string (YYYY-MM-DD) or None if not found.
    """
    try:
        dir_name = filing_path.name
        parts = dir_name.split("-")
        year = None

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

            date_patterns = [
                r"FILED AS OF DATE:\s*(\d{8})",
                r"CONFORMED PERIOD OF REPORT:\s*(\d{8})",
                r"DATE AS OF CHANGE:\s*(\d{8})",
            ]

            for pattern in date_patterns:
                match = re.search(pattern, content)
                if match:
                    date_str = match.group(1)
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        # Fallback: use year from accession number
        if year:
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
        counts["filing_date"] = extract_filing_date(filing_path)

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

        with open(main_file, "r", encoding="utf-8", errors="ignore") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        text = soup.get_text().lower()

        # Count non-accrual variations
        non_accrual_patterns = [r"non-accrual", r"nonaccrual", r"non\s+accrual"]
        for pattern in non_accrual_patterns:
            matches = re.findall(pattern, text)
            counts["non_accrual_count"] += len(matches)

        # Count payment default variations
        payment_default_patterns = [r"payment\s+default", r"payment-default"]
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
        Tuple of (signal_str, color_indicator).
    """
    if len(quarterly_data) < 2:
        return "INSUFFICIENT_DATA", "[?]"

    counts = [q["non_accrual_count"] for q in quarterly_data]

    first_half_avg = sum(counts[:len(counts)//2]) / max(1, len(counts)//2)
    second_half_avg = sum(counts[len(counts)//2:]) / max(1, len(counts) - len(counts)//2)

    change = counts[-1] - counts[0]

    if second_half_avg > first_half_avg * 1.1 or change > 10:
        return "DETERIORATING", "[!]"
    elif second_half_avg < first_half_avg * 0.9 or change < -10:
        return "IMPROVING", "[+]"
    else:
        return "STABLE", "[=]"


def create_risk_record(filing_data, quarter_label, fund_name):
    """Create a risk record from filing analysis.

    Args:
        filing_data: Dictionary with keyword counts and filing date.
        quarter_label: Label for this quarter (e.g., "Q1 2024").
        fund_name: Name of the BDC fund.

    Returns:
        Dictionary matching the database schema for bdc_loans.
    """
    filing_date = filing_data.get("filing_date") or datetime.now().strftime("%Y-%m-%d")

    return {
        "borrower": f"{fund_name} Portfolio ({quarter_label})",
        "fund": fund_name,
        "sector": "Diversified",
        "cost": 0,
        "fair_value": filing_data["non_accrual_count"],
        "date_added": filing_date
    }


def process_single_bdc(bdc, bdc_index, total_bdcs):
    """Process a single BDC - download filings and analyze.

    Args:
        bdc: Dictionary with ticker and name.
        bdc_index: Current index (1-based) for progress display.
        total_bdcs: Total number of BDCs being processed.

    Returns:
        Tuple of (records_saved, quarterly_data, trend_str, signal).
    """
    ticker = bdc["ticker"]
    name = bdc["name"]

    print(f"\nProcessing {bdc_index}/{total_bdcs}: {name} ({ticker})...", end=" ", flush=True)
    logger.info(f"Processing BDC {bdc_index}/{total_bdcs}: {name} ({ticker})")

    try:
        # Download filings
        filing_paths = download_10q_filings(ticker, limit=NUM_QUARTERS)

        if not filing_paths:
            print("No filings found.")
            logger.warning(f"No filings found for {ticker}")
            return 0, [], "N/A", "NO_DATA"

        # Analyze each filing
        quarterly_data = []
        for filing_path in filing_paths:
            data = count_distress_keywords(filing_path)
            quarterly_data.append(data)

        # Sort by filing date (oldest first)
        quarterly_data.sort(key=lambda x: x.get("filing_date") or "")

        # Determine trend signal
        signal, color = determine_trend_signal(quarterly_data)

        # Build trend string
        trend_parts = []
        for i, q in enumerate(quarterly_data):
            count = q["non_accrual_count"]
            trend_parts.append(f"Q{i+1}({count})")
        trend_str = " -> ".join(trend_parts)

        # Save records to database
        saved_count = 0
        for i, data in enumerate(quarterly_data):
            quarter_label = f"Q{i+1}"
            if data.get("filing_date"):
                try:
                    dt = datetime.strptime(data["filing_date"], "%Y-%m-%d")
                    q_num = (dt.month - 1) // 3 + 1
                    quarter_label = f"Q{q_num} {dt.year}"
                except:
                    pass

            record = create_risk_record(data, quarter_label, name)

            try:
                save_loan(record)
                saved_count += 1
            except Exception as e:
                logger.error(f"Failed to save record for {name}: {e}")

        print(f"Done. {color} {signal}")
        logger.info(f"{name}: {trend_str} - Signal: {signal} - Saved: {saved_count} records")

        return saved_count, quarterly_data, trend_str, signal

    except Exception as e:
        print(f"ERROR: {e}")
        logger.error(f"Failed to process {ticker}: {e}")
        return 0, [], "ERROR", "FAILED"


def run_scraper():
    """Run the BDC scraper pipeline for all BDCs in the universe.

    Downloads the last 4 10-Q filings for each BDC,
    analyzes distress trends, and saves records to the database.

    Returns:
        Total number of records saved.
    """
    try:
        logger.info("Starting BDC scraper run (Multi-Fund Trend Analysis)")
        print("\n" + "="*60)
        print("BDC UNIVERSE DISTRESS TREND ANALYSIS")
        print("="*60)
        print(f"Analyzing {len(BDC_UNIVERSE)} BDCs: {', '.join(b['ticker'] for b in BDC_UNIVERSE)}")

        # Initialize database
        init_db()

        total_saved = 0
        results = []

        for i, bdc in enumerate(BDC_UNIVERSE, 1):
            saved, quarterly_data, trend_str, signal = process_single_bdc(bdc, i, len(BDC_UNIVERSE))
            total_saved += saved
            results.append({
                "name": bdc["name"],
                "ticker": bdc["ticker"],
                "trend": trend_str,
                "signal": signal,
                "records": saved
            })

            # Rate limiting - be polite to SEC servers
            if i < len(BDC_UNIVERSE):
                print(f"   (Rate limit: waiting {SEC_RATE_LIMIT_DELAY}s before next request...)")
                time.sleep(SEC_RATE_LIMIT_DELAY)

        # Print summary
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        for r in results:
            status = "[!]" if r["signal"] == "DETERIORATING" else "[+]" if r["signal"] == "IMPROVING" else "[=]"
            print(f"{r['ticker']:6} | {r['name']:30} | {status} {r['signal']:15} | {r['records']} records")
        print("="*60)
        print(f"Total records saved: {total_saved}")

        logger.info(f"BDC scraper completed: {total_saved} total records saved across {len(BDC_UNIVERSE)} BDCs")

        return total_saved

    except Exception as e:
        logger.error(f"BDC scraper failed: {e}")
        print(f"\nERROR: {e}")
        return 0


if __name__ == "__main__":
    print("Running BDC Scraper (Top 5 BDC Universe)...")
    count = run_scraper()
    if count > 0:
        print(f"\nSuccessfully saved {count} quarterly risk records to database")
    else:
        print("Failed to save risk records - check scraping_log.txt")
    print(f"Log file: {LOG_PATH}")
