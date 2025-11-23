# -*- coding: utf-8 -*-


import gspread      #note gspread  and the google.oauth2.service account to get credentials
import pandas as pd
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import csv
import yfinance as yf
import os
from pathlib import Path
import sys
import numpy as np
import traceback



# Google Drive OAuth setup
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

with open("client_secret.json", "w") as f:
    f.write(os.environ["OAUTH_JSON"])  

flow = InstalledAppFlow.from_client_secrets_file("client_secret.json", SCOPES)
creds = flow.run_local_server(port=0)  # Run locally once to generate token

# Save token.json for reuse (optional)
import pickle
with open("token.pkl", "wb") as token_file:
    pickle.dump(creds, token_file)

# Build Drive service
drive_service = build('drive', 'v3', credentials=creds)

#client = gspread.authorize(creds)
#sheet = client.open("Cleaned_companies_daily_data_automated")
#worksheet = sheet.sheet1


"""### First script"""


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
output_csv_file = 'data/companies_daily_data_automated_first_script.csv'
if not os.path.exists(output_csv_file):
  try:
    # Use 'x' mode: exclusive creation mode. Fails if the file exists.
    with open(output_csv_file, 'x') as f:
        print(f"File '{output_csv_file}' created successfully.")
  except FileExistsError:
    print(f"File '{output_csv_file}' already exists.")
  except FileNotFoundError:
    # This might occur if the directory path itself doesn't exist
    print(f"Error: Directory not found for '{output_csv_file}'")

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

            # Append data to the CSV
            output_path = "data/output_csv_file"

            data.to_csv("data/output_csv_file.csv",  mode='a', header=not os.path.exists("data/output_csv_file"))
            print(f"Successfully downloaded and appended data for {ticker}")
        else:
            print(f"No new data found for {ticker} with start date {start_date}")
    except Exception as e:
        print(f"Error downloading data for {ticker}: {e}")
    return data

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

#check the data shape
automated_data_test = pd.read_csv("data/output_csv_file.csv")
print(automated_data_test.shape)

### Clean the data
def process_and_upload_data(csv_path: str):
  try :
    # Create a copy of the DataFrame to avoid modifying the original
    cleaned_automated_data = pd.read_csv(csv_path)

    # Remove the first row (index 0) which contains the Ticker information
    cleaned_automated_data = cleaned_automated_data.iloc[1:].copy()

    # Rename the 'Price' column to 'Date'
    cleaned_automated_data = cleaned_automated_data.rename(columns={'Price': 'Date'})

    # Remove duplicate (ticker, date) pairs, keep the latest appended row
    cleaned_automated_data = cleaned_automated_data.drop_duplicates(subset=['Company (Ticker)', 'Date'], keep='last').copy()

    # Save the cleaned data to local CSV (optional, but part of original workflow)
    cleaned_automated_data.to_csv("data/Cleaned_companies_daily_data_automated.csv", index=False)

    # Display the first few rows of the cleaned DataFrame to verify the changes
    print("Data Shape:", cleaned_automated_data.shape)
    print(cleaned_automated_data.tail())

    # --- Data type conversion and NaN/Inf handling for gspread ---

    # Convert 'Date' column to datetime, then to string.
    cleaned_automated_data['Date'] = pd.to_datetime(cleaned_automated_data['Date'], errors='coerce', format='mixed')
    cleaned_automated_data['Date'] = cleaned_automated_data['Date'].dt.strftime('%Y-%m-%d')

    print("changed date type:", cleaned_automated_data.shape)
    print(cleaned_automated_data.info())
    # Process numeric columns: convert to numeric, handle inf/nan.
    numeric_cols = ['Close', 'High', 'Low', 'Open', 'Volume']
    for col in numeric_cols:
        try:
          cleaned_automated_data[col] = pd.to_numeric(cleaned_automated_data[col], errors='coerce')
          # Replace inf and -inf with NaN.
          cleaned_automated_data[col] = cleaned_automated_data[col].replace([np.inf, -np.inf], np.nan)
          # For gspread, replace NaN with empty string.
          cleaned_automated_data[col] = cleaned_automated_data[col].fillna('')
        except Exception as e:
          print(f"\nERROR during processing of column '{col}': {e}")
          print("Traceback:", traceback.format_exc())

    # Convert ALL data in the DataFrame to string representation
    # This ensures that no non-JSON-compliant types are passed to gspread.
    #cleaned_automated_data = cleaned_automated_data.astype(str)
    cleaned_automated_data = cleaned_automated_data.where(cleaned_automated_data.notna(), '')

    #save again
    csv_file = "automated_cleaned_companies_daily_data_.csv"
    cleaned_automated_data.to_csv(csv_file, index=False)

    folder_id = "19HTDUml3bn3XUOeZWi09jd9xb0-x6FJ-"
    file_metadata = {
       "name": csv_file,
       "parents": [folder_id]  # this saves it inside the folder
     }
    media = MediaFileUpload(csv_file, mimetype="text/csv")
    file = drive_service.files().create(
         body=file_metadata,
         media_body=media,
         fields="id"
     ).execute()

    print(f"Uploaded CSV to Drive with file ID: {file['id']}")    

    # Convert DataFrame to a list of lists, including headers
    # Now, all values in cleaned_automated_data should be strings
    #data_to_upload = [cleaned_automated_data.columns.values.tolist()] + cleaned_automated_data.values.tolist()
        


    #Trying  to save in csv so that it opens as csv in googlesheet
    #with open("Cleaned_companies_daily_data_automated.csv", "r") as f:
      #reader = list(csv.reader(f))

    # Clear existing content and then update the sheet
    #worksheet.clear()
    #worksheet.update(values=data_to_upload, range_name='A1')
    #worksheet.update(data_to_upload, value_input_option='USER_ENTERED')
    #worksheet.update(reader, value_input_option="USER_ENTERED")

    #print("Data successfully uploaded to Google Sheet.")
  except Exception as e:
    print(f"\nERROR during initial data loading or cleaning steps: {e}")
    print("Traceback:", traceback.format_exc())

# Call the function to execute the process
process_and_upload_data("data/output_csv_file.csv")
