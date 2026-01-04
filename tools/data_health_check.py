"""Data Health Check for Shadow Bank Risk Observatory."""

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "risk_data.db"

# Track health score
checks_passed = 0
checks_total = 0


def log_check(passed, message, is_warning=False):
    """Log a check result with indicator."""
    global checks_passed, checks_total
    checks_total += 1

    if passed:
        checks_passed += 1
        print(f"  [PASS] {message}")
    elif is_warning:
        print(f"  [WARN] {message}")
    else:
        print(f"  [FAIL] {message}")


def get_connection():
    """Get a connection to the SQLite database."""
    if not DB_PATH.exists():
        print(f"❌ Database not found at {DB_PATH}")
        sys.exit(1)
    return sqlite3.connect(DB_PATH)


def volume_check(cursor):
    """Check row counts for each table."""
    print("\n" + "=" * 60)
    print("1. VOLUME CHECK")
    print("=" * 60)

    tables = ["warn_notices", "legal_cases", "bdc_loans"]
    counts = {}

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        counts[table] = count

        if count == 0:
            log_check(False, f"{table}: {count} rows (EMPTY TABLE)")
        else:
            log_check(True, f"{table}: {count} rows")

    return counts


def freshness_check(cursor):
    """Check date freshness for each table."""
    print("\n" + "=" * 60)
    print("2. FRESHNESS CHECK")
    print("=" * 60)

    today = datetime.now().date()
    thirty_days_ago = today - timedelta(days=30)

    date_columns = {
        "warn_notices": "date_filed",
        "legal_cases": "date_filed",
        "bdc_loans": "date_added"
    }

    for table, date_col in date_columns.items():
        cursor.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}")
        result = cursor.fetchone()
        min_date, max_date = result[0], result[1]

        if not min_date or not max_date:
            log_check(False, f"{table}: No date data available")
            continue

        print(f"  {table}: {min_date} to {max_date}")

        # Check for stale data
        try:
            latest = datetime.strptime(max_date, "%Y-%m-%d").date()
            if latest < thirty_days_ago:
                log_check(False, f"{table}: Data is stale (latest: {max_date})", is_warning=True)
            else:
                log_check(True, f"{table}: Data is fresh (within 30 days)")

            # Check for future dates
            if latest > today:
                log_check(False, f"{table}: Future date detected! ({max_date})")
            else:
                log_check(True, f"{table}: No future dates")
        except ValueError:
            log_check(False, f"{table}: Invalid date format detected")


def content_quality_check(cursor):
    """Check content quality for each table."""
    print("\n" + "=" * 60)
    print("3. CONTENT QUALITY CHECK")
    print("=" * 60)

    # WARN: Check employees
    print("\n  [WARN Notices]")
    cursor.execute("SELECT AVG(employees), MIN(employees), MAX(employees) FROM warn_notices")
    result = cursor.fetchone()
    avg_emp, min_emp, max_emp = result

    if avg_emp is not None:
        print(f"  Employees - Avg: {avg_emp:.1f}, Min: {min_emp}, Max: {max_emp}")
        if min_emp is not None and min_emp < 0:
            log_check(False, "Negative employee counts detected!")
        else:
            log_check(True, "Employee counts are valid (non-negative)")
    else:
        log_check(False, "No employee data available")

    # Legal: Check courts
    print("\n  [Legal Cases]")
    cursor.execute("SELECT DISTINCT court FROM legal_cases")
    courts = [row[0] for row in cursor.fetchall()]
    valid_courts = [c for c in courts if c and c.upper() not in ("N/A", "NA", "NONE", "")]

    print(f"  Unique courts: {len(courts)} total, {len(valid_courts)} valid")
    if len(valid_courts) == 0 and len(courts) > 0:
        log_check(False, "All court names are N/A or invalid")
    elif len(valid_courts) > 0:
        log_check(True, f"Valid court data: {', '.join(valid_courts[:3])}...")
    else:
        log_check(False, "No court data available", is_warning=True)

    # BDC: Check fair_value
    print("\n  [BDC Loans]")
    cursor.execute("SELECT COUNT(*) FROM bdc_loans WHERE fair_value IS NULL")
    null_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM bdc_loans")
    total_count = cursor.fetchone()[0]

    if total_count > 0:
        null_pct = (null_count / total_count) * 100
        print(f"  Fair value nulls: {null_count}/{total_count} ({null_pct:.1f}%)")

        if null_count == 0:
            log_check(True, "All fair_value fields have data")
        elif null_pct < 10:
            log_check(True, f"Fair value mostly populated ({100 - null_pct:.1f}%)", is_warning=True)
        else:
            log_check(False, f"High null rate in fair_value ({null_pct:.1f}%)")
    else:
        log_check(False, "No BDC loan data available", is_warning=True)


def business_logic_check(cursor):
    """Check for duplicates and data junk."""
    print("\n" + "=" * 60)
    print("4. BUSINESS LOGIC VALIDATION")
    print("=" * 60)

    # Check for duplicates (using unique constraints, these shouldn't exist)
    print("\n  [Duplicate Check]")

    # WARN duplicates
    cursor.execute("""
        SELECT company, date_filed, COUNT(*) as cnt
        FROM warn_notices
        GROUP BY company, date_filed
        HAVING cnt > 1
    """)
    warn_dups = cursor.fetchall()
    if warn_dups:
        log_check(False, f"WARN: {len(warn_dups)} duplicate company+date combinations")
    else:
        log_check(True, "WARN: No duplicates detected")

    # Legal duplicates
    cursor.execute("""
        SELECT defendant, date_filed, COUNT(*) as cnt
        FROM legal_cases
        GROUP BY defendant, date_filed
        HAVING cnt > 1
    """)
    legal_dups = cursor.fetchall()
    if legal_dups:
        log_check(False, f"Legal: {len(legal_dups)} duplicate defendant+date combinations")
    else:
        log_check(True, "Legal: No duplicates detected")

    # BDC duplicates
    cursor.execute("""
        SELECT borrower, fund, date_added, COUNT(*) as cnt
        FROM bdc_loans
        GROUP BY borrower, fund, date_added
        HAVING cnt > 1
    """)
    bdc_dups = cursor.fetchall()
    if bdc_dups:
        log_check(False, f"BDC: {len(bdc_dups)} duplicate borrower+fund+date combinations")
    else:
        log_check(True, "BDC: No duplicates detected")

    # Check for data junk (null/empty/nan company names)
    print("\n  [Data Junk Check]")

    cursor.execute("""
        SELECT COUNT(*) FROM warn_notices
        WHERE company IS NULL OR TRIM(company) = '' OR LOWER(company) = 'nan'
    """)
    warn_junk = cursor.fetchone()[0]
    if warn_junk > 0:
        log_check(False, f"WARN: {warn_junk} records with invalid company names")
    else:
        log_check(True, "WARN: All company names are valid")

    cursor.execute("""
        SELECT COUNT(*) FROM legal_cases
        WHERE defendant IS NULL OR TRIM(defendant) = '' OR LOWER(defendant) = 'nan'
    """)
    legal_junk = cursor.fetchone()[0]
    if legal_junk > 0:
        log_check(False, f"Legal: {legal_junk} records with invalid defendant names")
    else:
        log_check(True, "Legal: All defendant names are valid")

    cursor.execute("""
        SELECT COUNT(*) FROM bdc_loans
        WHERE borrower IS NULL OR TRIM(borrower) = '' OR LOWER(borrower) = 'nan'
    """)
    bdc_junk = cursor.fetchone()[0]
    if bdc_junk > 0:
        log_check(False, f"BDC: {bdc_junk} records with invalid borrower names")
    else:
        log_check(True, "BDC: All borrower names are valid")


def print_health_score():
    """Print the final health score."""
    print("\n" + "=" * 60)
    print("HEALTH SCORE")
    print("=" * 60)

    if checks_total == 0:
        print("  No checks performed!")
        return 0

    score = int((checks_passed / checks_total) * 100)

    if score >= 90:
        indicator = "[+++]"
        status = "EXCELLENT"
    elif score >= 70:
        indicator = "[++]"
        status = "GOOD"
    elif score >= 50:
        indicator = "[+]"
        status = "NEEDS ATTENTION"
    else:
        indicator = "[!]"
        status = "CRITICAL"

    print(f"\n  {indicator} {score}/100 - {status}")
    print(f"  ({checks_passed}/{checks_total} checks passed)")

    return score


def run_health_check():
    """Run all health checks and print report."""
    print("\n" + "=" * 60)
    print("  SHADOW BANK DATA HEALTH CHECK")
    print("=" * 60)
    print(f"  Database: {DB_PATH}")
    print(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        volume_check(cursor)
        freshness_check(cursor)
        content_quality_check(cursor)
        business_logic_check(cursor)
        score = print_health_score()

        print("\n" + "=" * 60)
        return score

    finally:
        conn.close()


if __name__ == "__main__":
    run_health_check()
