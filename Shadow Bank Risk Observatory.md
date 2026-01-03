1. Master PRD: Shadow Bank Risk Observatory
Product Name: Shadow Bank Risk Observatory (MVP) Type: Data Intelligence Platform & Newsletter Generator Stack: Python, SQLite, Streamlit, GitHub Actions

1. Executive Summary
We are building a "Smoke Detector" for the private credit industry. Since private funds are opaque, we monitor their public proxies: Business Development Companies (BDCs) for asset valuations, WARN Act notices for portfolio company layoffs, and Federal Court dockets for distress litigation.

2. Core Features
A. The Data Engine (Backend)
Three independent scrapers running on a daily schedule:

BDC Monitor: Downloads SEC 10-K/10-Q filings for top BDCs (e.g., Ares, OWL). Parses the "Schedule of Investments" to identify loans marked below 85% of cost.

Labor Monitor: Scrapes state WARN Act sites (NY, CA, TX) for layoffs >50 employees in relevant sectors.

Litigation Monitor: Queries CourtListener API for "Breach of Contract" or "Receivership" cases involving entities on our watchlist.

B. The Storage Layer
Database: risk_data.db (SQLite).

Schema: Three main tables: loans, layoffs, litigation.

Logic: Upsert capabilities to prevent duplicate alerts (based on unique keys like loan_id or case_number).

C. The User Interfaces (Frontend)
Analyst Dashboard (Streamlit):

Interactive view of the database.

KPI Ticker: Aggregate "Distress Score."

Visuals: Sector heatmaps and markdown waterfall charts.

Newsletter Generator:

A script that reads today's new entries and compiles a daily_report.md formatted for easy copy-pasting into Substack.

3. Architecture Diagram
GitHub Actions (Cron) → Run Scrapers → Update SQLite DB → Deploy Streamlit App & Generate MD Report