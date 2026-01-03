"""Shadow Bank Risk Observatory - Streamlit Dashboard."""

import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

DB_PATH = Path(__file__).parent.parent / "data" / "risk_data.db"


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def get_connection():
    """Get a connection to the SQLite database."""
    return sqlite3.connect(DB_PATH)


def load_loans():
    """Load all BDC loans from the database."""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM bdc_loans", conn)
    conn.close()
    return df


def load_warn_notices():
    """Load all WARN notices from the database."""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM warn_notices", conn)
    conn.close()
    return df


def load_legal_cases():
    """Load all legal cases from the database."""
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM legal_cases", conn)
    conn.close()
    return df


def get_distressed_loans_count():
    """Count loans where fair_value < cost (distressed)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM bdc_loans
        WHERE fair_value < cost
    """)
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_total_layoffs():
    """Sum of all employees affected by WARN notices."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(SUM(employees), 0) FROM warn_notices")
    total = cursor.fetchone()[0]
    conn.close()
    return total


def get_legal_cases_count():
    """Count of legal cases in the database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM legal_cases")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_layoffs_by_state():
    """Get layoff totals grouped by state."""
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT state, SUM(employees) as total_employees
        FROM warn_notices
        GROUP BY state
        ORDER BY total_employees DESC
    """, conn)
    conn.close()
    return df


# =============================================================================
# SCRAPER SIMULATION
# =============================================================================

def refresh_data():
    """Run the scraper scripts to refresh data."""
    from scrapers.bdc_scraper import run_scraper as run_bdc
    from scrapers.warn_scraper import run_scraper as run_warn
    from scrapers.legal_scraper import run_scraper as run_legal

    with st.spinner("Running BDC Scraper (SEC 10-Q Analysis)..."):
        bdc_count = run_bdc()

    with st.spinner("Running WARN Scraper (NY State)..."):
        warn_count = run_warn()

    with st.spinner("Running Legal Scraper (CourtListener)..."):
        legal_count = run_legal()

    return bdc_count, warn_count, legal_count


# =============================================================================
# STREAMLIT APP
# =============================================================================

st.set_page_config(
    page_title="Shadow Bank Risk Observatory",
    page_icon="🏦",
    layout="wide"
)

# Header
st.title("Shadow Bank Risk Observatory")
st.markdown("*Financial Risk Monitoring Dashboard*")

st.divider()

# Refresh Button
col_refresh, col_spacer = st.columns([1, 5])
with col_refresh:
    if st.button("Refresh Data", type="primary"):
        bdc_count, warn_count, legal_count = refresh_data()
        st.success(f"Refreshed: {bdc_count} BDC records, {warn_count} WARN notices, {legal_count} legal cases")
        st.rerun()

# Top Row - Key Metrics
st.subheader("Key Risk Indicators")

col1, col2, col3 = st.columns(3)

with col1:
    distressed = get_distressed_loans_count()
    st.metric(
        label="Distressed Loans",
        value=distressed,
        help="Loans where fair value is less than cost"
    )

with col2:
    layoffs = get_total_layoffs()
    st.metric(
        label="Total Layoffs",
        value=f"{layoffs:,}",
        help="Total employees affected by WARN notices"
    )

with col3:
    lawsuits = get_legal_cases_count()
    st.metric(
        label="New Lawsuits",
        value=lawsuits,
        help="Legal cases tracked in the system"
    )

# Credit Stress Velocity Chart (BDC Trend)
st.markdown("---")
st.markdown("#### Credit Stress Velocity (Ares Capital)")

# Load and filter BDC data for Ares Capital
loans_df_full = load_loans()
ares_df = loans_df_full[loans_df_full["fund"] == "Ares Capital"].copy()

if not ares_df.empty and len(ares_df) > 1:
    # Sort by date and ensure fair_value is numeric
    ares_df["date_added"] = pd.to_datetime(ares_df["date_added"])
    ares_df["fair_value"] = pd.to_numeric(ares_df["fair_value"], errors="coerce")
    ares_df = ares_df.sort_values("date_added")

    # Create chart data
    chart_data = ares_df.set_index("date_added")[["fair_value"]].rename(
        columns={"fair_value": "Distress Score (Non-Accrual Count)"}
    )

    st.line_chart(chart_data)

    # Calculate trend for annotation
    start_value = ares_df["fair_value"].iloc[0]
    latest_value = ares_df["fair_value"].iloc[-1]

    if start_value > 0:
        pct_change = ((latest_value - start_value) / start_value) * 100

        if latest_value > start_value:
            st.warning(f"Trend Alert: Credit stress has increased by {pct_change:.1f}% over the last {len(ares_df)} quarters.")
        else:
            st.success("Trend Alert: Credit stress is stable or improving.")
    else:
        st.info("Trend Alert: Baseline data unavailable for comparison.")
elif not ares_df.empty:
    st.info(f"Only {len(ares_df)} data point(s) available. Need at least 2 quarters for trend analysis.")
else:
    st.info("No Ares Capital trend data available. Run the BDC scraper to populate.")

st.divider()

# Charts Section
st.subheader("Layoffs by State")

layoffs_df = get_layoffs_by_state()

if not layoffs_df.empty:
    st.bar_chart(
        layoffs_df.set_index("state")["total_employees"]
    )
else:
    st.info("No WARN notice data available. Click 'Refresh Data' to load.")

st.divider()

# Data Feed Section
st.subheader("Data Feed")

tab_loans, tab_warn, tab_legal = st.tabs(["📊 BDC Loans", "⚠️ WARN Notices", "⚖️ Legal Cases"])

with tab_loans:
    loans_df = load_loans()
    if not loans_df.empty:
        # Format currency columns
        loans_display = loans_df.copy()
        loans_display["cost"] = loans_display["cost"].apply(lambda x: f"${x:,.2f}")
        loans_display["fair_value"] = loans_display["fair_value"].apply(lambda x: f"${x:,.2f}")
        st.dataframe(loans_display, hide_index=True)
        st.caption(f"Total records: {len(loans_df)}")
    else:
        st.info("No loan data available.")

with tab_warn:
    warn_df = load_warn_notices()
    if not warn_df.empty:
        st.dataframe(warn_df, hide_index=True)
        st.caption(f"Total records: {len(warn_df)}")
    else:
        st.info("No WARN notice data available.")

with tab_legal:
    legal_df = load_legal_cases()
    if not legal_df.empty:
        st.dataframe(legal_df, hide_index=True)
        st.caption(f"Total records: {len(legal_df)}")
    else:
        st.info("No legal case data available.")

# Footer
st.divider()
st.caption("Shadow Bank Risk Observatory | Data refreshed from SEC EDGAR & State WARN databases")
