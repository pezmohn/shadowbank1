"""Generate daily newsletter for Shadow Bank Risk Observatory."""

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Fix Windows console encoding for emojis
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = Path(__file__).parent / "data" / "risk_data.db"
OUTPUT_PATH = Path(__file__).parent / "daily_report.md"


def get_connection():
    """Get a connection to the SQLite database."""
    return sqlite3.connect(DB_PATH)


def get_todays_loans():
    """Get all BDC loans added today."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT borrower, fund, sector, cost, fair_value
        FROM bdc_loans
        WHERE date_added = ?
    """, (today,))
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_todays_warns():
    """Get all WARN notices filed today."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT company, state, employees
        FROM warn_notices
        WHERE date_filed = ?
    """, (today,))
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_todays_legal():
    """Get all legal cases filed today."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT defendant, plaintiff, court, case_type
        FROM legal_cases
        WHERE date_filed = ?
    """, (today,))
    rows = cursor.fetchall()
    conn.close()
    return rows


def format_currency(value):
    """Format a number as currency."""
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:,.0f}"


def generate_newsletter():
    """Generate the daily newsletter in Markdown format."""
    today = datetime.now().strftime("%B %d, %Y")

    loans = get_todays_loans()
    warns = get_todays_warns()
    legal = get_todays_legal()

    # Calculate summary stats
    total_layoffs = sum(w[2] for w in warns) if warns else 0
    distressed_loans = [l for l in loans if l[4] < l[3]]  # fair_value < cost
    total_distressed_value = sum(l[3] - l[4] for l in distressed_loans)

    # Build the newsletter
    lines = []

    # Header
    lines.append(f"# 🏦 Shadow Bank Risk Observatory")
    lines.append(f"## Daily Risk Report — {today}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Executive Summary
    lines.append("## 📊 Executive Summary")
    lines.append("")
    lines.append(f"- **Distressed Loans Tracked:** {len(distressed_loans)}")
    lines.append(f"- **Total Impairment:** {format_currency(total_distressed_value)}")
    lines.append(f"- **Layoff Notices:** {len(warns)} companies ({total_layoffs:,} employees)")
    lines.append(f"- **New Legal Cases:** {len(legal)}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # BDC Loans Section
    lines.append("## 📉 BDC Portfolio Alerts")
    lines.append("")
    if loans:
        # Group by sector
        sectors = {}
        for borrower, fund, sector, cost, fair_value in loans:
            if sector not in sectors:
                sectors[sector] = []
            sectors[sector].append((borrower, fund, cost, fair_value))

        for sector, sector_loans in sectors.items():
            lines.append(f"### {sector}")
            lines.append("")
            for borrower, fund, cost, fair_value in sector_loans:
                change = fair_value - cost
                change_pct = (change / cost) * 100 if cost else 0
                status = "🔴" if change < 0 else "🟢"
                lines.append(f"- {status} **{borrower}** ({fund})")
                lines.append(f"  - Cost: {format_currency(cost)} → Fair Value: {format_currency(fair_value)} ({change_pct:+.1f}%)")
            lines.append("")
    else:
        lines.append("*No new loan data recorded today.*")
        lines.append("")

    lines.append("---")
    lines.append("")

    # WARN Notices Section
    lines.append("## 🚨 WARN Act Layoff Notices")
    lines.append("")
    if warns:
        # Group by state
        states = {}
        for company, state, employees in warns:
            if state not in states:
                states[state] = []
            states[state].append((company, employees))

        for state, state_warns in sorted(states.items(), key=lambda x: -sum(w[1] for w in x[1])):
            state_total = sum(w[1] for w in state_warns)
            lines.append(f"### {state} ({state_total:,} employees)")
            lines.append("")
            for company, employees in state_warns:
                lines.append(f"- ⚠️ **{company}** — {employees:,} employees")
            lines.append("")
    else:
        lines.append("*No WARN notices filed today.*")
        lines.append("")

    lines.append("---")
    lines.append("")

    # Legal Cases Section
    lines.append("## ⚖️ Legal & Litigation Watch")
    lines.append("")
    if legal:
        # Group by case type
        case_types = {}
        for defendant, plaintiff, court, case_type in legal:
            if case_type not in case_types:
                case_types[case_type] = []
            case_types[case_type].append((defendant, plaintiff, court))

        for case_type, cases in case_types.items():
            lines.append(f"### {case_type or 'General'}")
            lines.append("")
            for defendant, plaintiff, court in cases:
                lines.append(f"- 📋 **{plaintiff}** v. **{defendant}**")
                lines.append(f"  - Court: {court}")
            lines.append("")
    else:
        lines.append("*No new legal cases filed today.*")
        lines.append("")

    lines.append("---")
    lines.append("")

    # Footer
    lines.append("*This report is auto-generated by Shadow Bank Risk Observatory.*")
    lines.append("")
    lines.append("*Data sources: SEC EDGAR, State WARN databases, Court records*")

    return "\n".join(lines)


def main():
    """Generate and save the daily newsletter."""
    print("Generating daily newsletter...\n")

    newsletter = generate_newsletter()

    # Save to file
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(newsletter)

    print(newsletter)
    print(f"\n{'=' * 60}")
    print(f"Newsletter saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
