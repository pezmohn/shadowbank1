"""Database manager for Shadow Bank Risk Observatory."""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "risk_data.db"


def get_connection():
    """Get a connection to the SQLite database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bdc_loans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            borrower TEXT NOT NULL,
            fund TEXT,
            sector TEXT,
            cost REAL,
            fair_value REAL,
            date_added TEXT,
            UNIQUE(borrower, fund, date_added)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS warn_notices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT NOT NULL,
            state TEXT,
            employees INTEGER,
            date_filed TEXT,
            UNIQUE(company, state, date_filed)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS legal_cases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            defendant TEXT NOT NULL,
            plaintiff TEXT,
            court TEXT,
            case_type TEXT,
            date_filed TEXT,
            UNIQUE(defendant, plaintiff, date_filed)
        )
    """)

    conn.commit()
    conn.close()


def save_loan(data):
    """Insert a loan record, ignoring duplicates.

    Args:
        data: dict with keys: borrower, fund, sector, cost, fair_value, date_added
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO bdc_loans
        (borrower, fund, sector, cost, fair_value, date_added)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        data.get("borrower"),
        data.get("fund"),
        data.get("sector"),
        data.get("cost"),
        data.get("fair_value"),
        data.get("date_added")
    ))

    conn.commit()
    conn.close()
    return cursor.lastrowid


def save_warn(data):
    """Insert a WARN notice record, ignoring duplicates.

    Args:
        data: dict with keys: company, state, employees, date_filed
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO warn_notices
        (company, state, employees, date_filed)
        VALUES (?, ?, ?, ?)
    """, (
        data.get("company"),
        data.get("state"),
        data.get("employees"),
        data.get("date_filed")
    ))

    conn.commit()
    conn.close()
    return cursor.lastrowid


def save_legal(data):
    """Insert a legal case record, ignoring duplicates.

    Args:
        data: dict with keys: defendant, plaintiff, court, case_type, date_filed
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO legal_cases
        (defendant, plaintiff, court, case_type, date_filed)
        VALUES (?, ?, ?, ?, ?)
    """, (
        data.get("defendant"),
        data.get("plaintiff"),
        data.get("court"),
        data.get("case_type"),
        data.get("date_filed")
    ))

    conn.commit()
    conn.close()
    return cursor.lastrowid


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
