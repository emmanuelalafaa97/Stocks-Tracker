import yfinance as yf
import pandas as pd
import os

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
    "PBR": "Energy",
    "TTE": "Energy",
    "SHEL": "Energy",
    "CSUAY": "Energy",
    "E": "Energy",
    "HSBC": "Financial Services",
    "JPM": "Financial Services",
    "GS": "Financial Services",
    "BAC": "Financial Services",
    "C": "Financial Services",
    "WFC": "Financial Services",
    "MS": "Financial Services",
    "V": "Financial Services",
    "MA": "Financial Services",
    "EA": "Communication Services",
    "VOD": "Communication Services",
    "VZ": "Communication Services",
    "SPOT": "Communication Services",
    "NFLX": "Communication Services",
    "TMUS": "Communication Services",
    "DIS": "Communication Services",
    "T": "Communication Services",
    "CMCSA": "Communication Services",
    "NTDOY": "Communication Services",
    "AMH": "Real Estate",
    "HNGKY": "Real Estate",
    "ESS": "Real Estate",
    "SUI": "Real Estate",
    "CNGKY": "Real Estate",
    "WARFY": "Real Estate",
    "BPYPP": "Real Estate",
    "EGP": "Real Estate",
    "MAA-PI": "Real Estate",
    "HST": "Real Estate",
    "JNJ": "Healthcare",
    "UNH": "Healthcare",
    "AZN": "Healthcare",
    "MRK": "Healthcare",
    "PFE": "Healthcare",
    "SNY": "Healthcare",
    "SMMNY": "Healthcare",
    "CI": "Healthcare",
    "BSX": "Healthcare",
    "NVS": "Healthcare"
}

# Define the output CSV file path
output_csv_file = 'companies_daily_data_automated_first_script.csv'

# Get the latest date from the existing CSV if it exists
latest_date = None
if os.path.exists(output_csv_file):
    try:
        existing_df = pd.read_csv(output_csv_file)
        existing_df['Date'] = pd.to_datetime(existing_df['Date'])
        latest_date = existing_df['Date'].max()
    except Exception as e:
        print(f"Error reading existing CSV: {e}")
        latest_date = None # Reset latest_date if there's an error reading the file

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

            # Append data to the CSV or rather google sheet
            output_path = "/content/drive/MyDrive/Stocks Project folder/output_csv_file.csv"
            data.to_csv(output_path, mode='a', header=not os.path.exists(output_path))
            #data.to_csv("/content/drive/MyDrive/Stocks Project folder/output_csv_file",  mode='a', header=not os.path.exists("/content/drive/MyDrive/Stocks Project folder/output_csv_file"))
            print(f"Successfully downloaded and appended data for {ticker}")
        else:
            print(f"No new data found for {ticker} with start date {start_date}")
    except Exception as e:
        print(f"Error downloading data for {ticker}: {e}")

# Determine the start date for downloading
download_start_date = None
if latest_date:
    download_start_date = (latest_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
else:
    download_start_date = '2010-01-01' # Start from the beginning if no existing file or error

# Download and append data for all companies
for ticker in company_sector.keys():
    download_and_append_data(ticker, start_date=download_start_date)

print("\nAutomation script finished.")
