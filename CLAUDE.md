# Shadow Bank Risk Observatory

## Project Overview
Financial Risk Monitoring system for tracking and analyzing shadow banking risks.

## Tech Stack
- Python 3.10
- Streamlit
- SQLite
- GitHub Actions

## Project Rules

### Modular Code
- Keep scrapers in the `scrapers/` folder

### Database
- Use `sqlite3` for local storage
- Database location: `data/risk_data.db`

### Error Handling
- Scrapers must never crash the main loop
- Log errors to `scraping_log.txt`

### Dependencies
- sec-edgar-downloader
- beautifulsoup4
- pandas
- streamlit
- requests
