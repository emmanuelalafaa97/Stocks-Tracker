# Automated Daily Stocks Tracker
Python | GitHub Actions | Google Sheets / Google Drive | Tableau

This project automatically downloads daily stock price data from Yahoo Finance, cleans the data, stores it in a structured dataset, and uploads the final output for downstream analytics (e.g., Tableau).
The entire workflow runs fully automated using GitHub Actions.

## Features
#### 1. Automated Data Collection
```
   Fetches daily OHLCV data using yfinance

   Supports multiple companies across sectors

   Adds industry metadata (Sector, Company Name, Ticker)
```

#### 2. Data Cleaning Pipeline
```
   Converts date formats properly

   Fixes numeric columns

   Removes duplicates

   Handles NaN/inf values

   Produces a consistent CSV dataset ready for analytics
```

3. Automated Upload to Google Drive (as CSV)

Because Google Sheets is not ideal for Tableau (OData required), the workflow uploads the cleaned CSV file directly to Google Drive, using:

OAuth 2.0 client ID

GitHub Secrets for authentication

Google Drive API

The uploaded CSV can be connected directly from Tableau using Google Drive connector (no OData needed).

## Project Structure
```
Stocks-Tracker/
│
├── Stocks-Updated-Scripts.py      # Main Python ETL script
├── requirements.txt               # Python dependencies
├── .github/workflows/main.yml     # GitHub Actions CI/CD pipeline
├── README.md                      # Documentation
│
└── credentials/
    └── (OAuth JSON stored securely in GitHub Secrets)
```

## How It Works
1. GitHub Action triggers daily

Every day at a set time, GitHub Actions runs:

on:
  schedule:
    - cron: "0 6 * * *"   # Runs daily

2. Script fetches and processes stock data

Downloads stock history

Merges company metadata

Cleans and standardizes data

3. CSV file is generated

Example output:

Cleaned_companies_daily_data_automated.csv

4. CSV is uploaded to Google Drive

Your GitHub workflow uses OAuth credentials to upload:

file_metadata = {
  "name": csv_file,
  "parents": [folder_id]
}
