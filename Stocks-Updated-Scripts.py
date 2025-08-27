import sys
import gspread
from google.oauth2.service_account import Credentials
import yfinance as yf
import pandas as pd
import os

# === Google Sheets Auth ===
cred_file = sys.argv[1]  # e.g. "cred.json"
scopes =  ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

creds = Credentials.from_service_account_file(cred_file, scopes=scopes)
client = gspread.authorize(creds)

# === Open Google Sheet ===
spreadsheet = client.open("Cleaned_companies_daily_data_automated")   # 👈 replace with your sheet name
worksheet = spreadsheet.sheet1             # first tab

# === Company dictionaries (company_sector and company_names) ===
# Define the dictionary of companies and their sectors
company_sector = {
    "AAPL": "Technology",
    "AMZN": "Technology",
    "BABA": "Technology",
    "CRM": "Technology",
    "META": "Technology",
    "GOOG": "Technology",
    "INTC": "Technology",
    "MSFT": "Technology",
    "NVDA": "Technology",
    "TSLA": "Technology",
    "XOM": "Energy",
    "CVX": "Energy",
    "COP": "Energy",
    "SLB": "Energy",
    "BP": "Energy",
    "PBR": "Petróleo Brasileiro S.A. - Petrobras",
    "TTE": "TotalEnergies SE",
    "SHEL": "Shell plc",
    "CSUAY": "China Shenhua Energy Company Limited",
    "E": "ENI S.p.A.",
    "HSBC": "HSBC Holdings plc",
    "JPM": "JPMorgan Chase & Co.",
    "GS": "The Goldman Sachs Group, Inc.",
    "BAC": "Bank of America Corporation",
    "C": "Citigroup Inc.",
    "WFC": "Wells Fargo & Company",
    "MS": "Morgan Stanley",
    "V": "Visa Inc.",
    "MA": "Mastercard Incorporated",
    "EA": "Electronic Arts Inc.",
    "VOD": "Vodafone Group Public Limited Company",
    "VZ": "Verizon Communications Inc.",
    "SPOT": "Spotify Technology S.A.",
    "NFLX": "Netflix, Inc.",
    "TMUS": "T-Mobile US, Inc.",
    "DIS": "The Walt Disney Company",
    "T": "AT&T Inc.",
    "CMCSA": "Comcast Corporation",
    "NTDOY": "Nintendo Co., Ltd.",
    "AMH": "American Homes 4 Rent",
    "HNGKY": "Hongkong Land Holdings Limited",
    "ESS": "Essex Property Trust, Inc.",
    "SUI": "Sun Communities, Inc.",
    "CNGKY": "CK Asset Holdings Limited",
    "WARFY": "The Wharf (Holdings) Limited",
    "BPYPP": "Brookfield Property Partners L.P.",
    "EGP": "EastGroup Properties, Inc.",
    "MAA-PI": "Mid-America Apartment Communities, Inc.",
    "HST": "Host Hotels & Resorts, Inc.",
    "JNJ": "Johnson & Johnson",
    "UNH": "UnitedHealth Group Incorporated",
    "AZN": "AstraZeneca PLC",
    "MRK": "Merck & Co., Inc.",
    "PFE": "Pfizer Inc.",
    "SNY": "Sanofi",
    "SMMNY": "Siemens Healthineers AG",
    "CI": "The Cigna Group",
    "BSX": "Boston Scientific Corporation",
    "NVS": "Novartis AG"
}

# Function to download and append data for a given ticker
def download_and_append_data(ticker, start_date=None):
    try:
        # Download data from the day after the latest date to today
        data = yf.download(ticker, start=start_date, end=pd.to_datetime('today').strftime('%Y-%m-%d'), interval='1d')
        if not data.empty:
            # Add a column for the company ticker
            data['Company (Ticker)'] = ticker
            # Add sector and company name
            data['Sector'] = company_sector.get(ticker, 'Unknown')
            # Assuming company_names dictionary is available or can be created
            company_names = {
                "AAPL": "Apple Inc.", "AMZN": "Amazon Inc.", "BABA": "Alibaba Group Holding Limited",
                "CRM": "Salesforce, Inc.", "META": "Meta Platforms, Inc.", "GOOG": "Alphabet Inc. (Class C)",
                "INTC": "Intel Corporation", "MSFT": "Microsoft Corporation", "NVDA": "NVIDIA Corporation",
                "TSLA": "Tesla, Inc.", "XOM": "Exxon Mobil Corporation", "CVX": "Chevron Corporation",
                "COP": "ConocoPhillips", "SLB": "Schlumberger Limited", "BP": "BP p.l.c.",
                "PBR": "Petróleo Brasileiro S.A. - Petrobras", "TTE": "TotalEnergies SE", "SHEL": "Shell plc",
                "CSUAY": "China Shenhua Energy Company Limited", "E": "ENI S.p.A.",
                "HSBC": "HSBC Holdings plc",
                "JPM": "JPMorgan Chase & Co.", "GS": "The Goldman Sachs Group, Inc.",
                "BAC": "Bank of America Corporation", "C": "Citigroup Inc.", "WFC": "Wells Fargo & Company",
                "MS": "Morgan Stanley", "V": "Visa Inc.", "MA": "Mastercard Incorporated",
                "EA": "Electronic Arts Inc.", "VOD": "Vodafone Group Public Limited Company",
                "VZ": "Verizon Communications Inc.", "SPOT": "Spotify Technology S.A.", "NFLX": "Netflix, Inc.",
                "TMUS": "T-Mobile US, Inc.", "DIS": "The Walt Disney Company", "T": "AT&T Inc.",
                "CMCSA": "Comcast Corporation", "NTDOY": "Nintendo Co., Ltd.", "AMH": "American Homes 4 Rent",
                "HNGKY": "Hongkong Land Holdings Limited", "ESS": "Essex Property Trust, Inc.",
                "SUI": "Sun Communities, Inc.", "CNGKY": "CK Asset Holdings Limited",
                "WARFY": "The Wharf (Holdings) Limited", "BPYPP": "Brookfield Property Partners L.P.",
                "EGP": "EastGroup Properties, Inc.", "MAA-PI": "Mid-America Apartment Communities, Inc.",
                "HST": "Host Hotels & Resorts, Inc.", "JNJ": "Johnson & Johnson",
                "UNH": "UnitedHealth Group Incorporated", "AZN": "AstraZeneca PLC", "MRK": "Merck & Co., Inc.",
                "PFE": "Pfizer Inc.", "SNY": "Sanofi", "SMMNY": "Siemens Healthineers AG",
                "CI": "The Cigna Group", "BSX": "Boston Scientific Corporation", "NVS": "Novartis AG"
            }
            data['Company name'] = company_names.get(ticker, 'Unknown')
    except Exception as e:
        print(f"Error downloading data for {ticker}: {e}")



# === Download, clean, and upload data ===
def download_clean_and_upload_data(ticker, start_date="2010-01-01"):
    try:
        # Download data
        df = yf.download(ticker, start=start_date, end=pd.to_datetime("today").strftime("%Y-%m-%d"), interval="1d")
        if df.empty:
            print(f"⚠️ No data found for {ticker}")
            return

        # Add extra info
        df["Company (Ticker)"] = ticker
        df["Sector"] = company_sector.get(ticker, "Unknown")
        company_names = {
                "AAPL": "Apple Inc.", "AMZN": "Amazon Inc.", "BABA": "Alibaba Group Holding Limited",
                "CRM": "Salesforce, Inc.", "META": "Meta Platforms, Inc.", "GOOG": "Alphabet Inc. (Class C)",
                "INTC": "Intel Corporation", "MSFT": "Microsoft Corporation", "NVDA": "NVIDIA Corporation",
                "TSLA": "Tesla, Inc.", "XOM": "Exxon Mobil Corporation", "CVX": "Chevron Corporation",
                "COP": "ConocoPhillips", "SLB": "Schlumberger Limited", "BP": "BP p.l.c.",
                "PBR": "Petróleo Brasileiro S.A. - Petrobras", "TTE": "TotalEnergies SE", "SHEL": "Shell plc",
                "CSUAY": "China Shenhua Energy Company Limited", "E": "ENI S.p.A.",
                "HSBC": "HSBC Holdings plc",
                "JPM": "JPMorgan Chase & Co.", "GS": "The Goldman Sachs Group, Inc.",
                "BAC": "Bank of America Corporation", "C": "Citigroup Inc.", "WFC": "Wells Fargo & Company",
                "MS": "Morgan Stanley", "V": "Visa Inc.", "MA": "Mastercard Incorporated",
                "EA": "Electronic Arts Inc.", "VOD": "Vodafone Group Public Limited Company",
                "VZ": "Verizon Communications Inc.", "SPOT": "Spotify Technology S.A.", "NFLX": "Netflix, Inc.",
                "TMUS": "T-Mobile US, Inc.", "DIS": "The Walt Disney Company", "T": "AT&T Inc.",
                "CMCSA": "Comcast Corporation", "NTDOY": "Nintendo Co., Ltd.", "AMH": "American Homes 4 Rent",
                "HNGKY": "Hongkong Land Holdings Limited", "ESS": "Essex Property Trust, Inc.",
                "SUI": "Sun Communities, Inc.", "CNGKY": "CK Asset Holdings Limited",
                "WARFY": "The Wharf (Holdings) Limited", "BPYPP": "Brookfield Property Partners L.P.",
                "EGP": "EastGroup Properties, Inc.", "MAA-PI": "Mid-America Apartment Communities, Inc.",
                "HST": "Host Hotels & Resorts, Inc.", "JNJ": "Johnson & Johnson",
                "UNH": "UnitedHealth Group Incorporated", "AZN": "AstraZeneca PLC", "MRK": "Merck & Co., Inc.",
                "PFE": "Pfizer Inc.", "SNY": "Sanofi", "SMMNY": "Siemens Healthineers AG",
                "CI": "The Cigna Group", "BSX": "Boston Scientific Corporation", "NVS": "Novartis AG"
            }
        df["Company name"] = company_names.get(ticker, "Unknown")

        # Reset index so Date becomes a column
        df = df.reset_index()

        # === Data Cleaning ===
        # Remove first row if it contains ticker info (like in your example)
        df = df.iloc[1:].copy() if len(df) > 1 else df

        # Rename 'Close' column to 'Price' if needed
        if 'Close' in df.columns:
            df = df.rename(columns={'Close': 'Price'})

        # Optional: reorder columns for clarity
        columns_order = ["Date", "Company (Ticker)", "Company name", "Sector", "Open", "High", "Low", "Price", "Adj Close", "Volume"]
        df = df[[col for col in columns_order if col in df.columns]]

        # Save locally as cleaned CSV (optional)
        output_dir = "./output_csv"
        os.makedirs(output_dir, exist_ok=True)
        cleaned_csv_path = os.path.join(output_dir, f"Cleaned_{ticker}_daily_data.csv")
        df.to_csv(cleaned_csv_path, index=False)

        # Convert to rows for gspread and upload
        rows = [df.columns.tolist()] + df.values.tolist()
        worksheet.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"✅ Uploaded cleaned {ticker} data to Google Sheet")

    except Exception as e:
        print(f"❌ Error with {ticker}: {e}")

# Run for all tickers
for ticker in company_sector.keys():
    download_clean_and_upload_data(ticker)

print("\n🚀 Automation script finished. Cleaned data is in Google Sheets now.")