import yfinance as yf
import pandas as pd
import time
import logging
from pymongo import MongoClient

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
collection_name = "asset_data"

# MongoDB Client Setup
client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

# Expanded list of diversified assets
assets = {
    "Technology": ["AAPL", "MSFT", "GOOGL", "IBM", "TSLA", "NVDA", "AMZN", "ADBE", "INTC", "META"],
    "Healthcare": ["JNJ", "PFE", "MRK", "UNH", "BMY", "AMGN", "ABBV", "GILD", "BIIB"],
    "Financial": ["JPM", "BAC", "C", "GS", "MS", "WFC", "AXP", "BK", "TFC", "BLK"],
    "Consumer Goods": ["PG", "KO", "PEP", "WMT", "COST", "MCD", "HD", "TGT", "NKE", "SBUX"],
    "Broad Market ETFs": ["SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "IVV", "SCHB", "VXF"],
    "Sector-Specific ETFs": ["XLF", "XLK", "XLE", "XLV", "XLY", "XLC", "XLRE", "XLB", "XLP", "XLU"],
    "International and Emerging Market ETFs": ["VEA", "VWO", "EFA", "EEM", "ACWI", "VXUS", "SPDW", "IEMG", "SCZ"],
    "Treasury Bond ETFs": ["TLT", "IEF", "SHY", "BND", "GOVT", "SCHR", "VGIT", "SPTL", "VGSH"],
    "Corporate Bond ETFs": ["LQD", "HYG", "AGG", "VCIT", "VCLT", "SJNK", "BKLN", "FLOT"],
}

# Function to fetch historical data for a given ticker
def fetch_data(ticker, start="2000-01-01", end="2023-12-31", interval="1d"):
    try:
        logging.info(f"Fetching raw data for {ticker}...")
        # Fetch non-adjusted data
        data = yf.Ticker(ticker).history(start=start, end=end, interval=interval, actions=False)
        
        if data.empty:
            logging.warning(f"No data found for {ticker}. Skipping...")
            return None
        
        # Include only relevant columns
        data = data[["Open", "High", "Low", "Close", "Volume"]].copy()
        data["Ticker"] = ticker  # Add ticker symbol
        data.reset_index(inplace=True)  # Reset index for MongoDB insertion
        return data
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return None

# Fetch data for all assets and save to MongoDB
for category, tickers in assets.items():
    for ticker in tickers:
        data = fetch_data(ticker)
        if data is not None:
            data["Category"] = category  # Add category for each asset
            data.dropna(inplace=True)  # Drop missing values
            data_records = data.to_dict("records")  # Convert to dictionary format
            
            # Insert in batches to avoid overloading
            for i in range(0, len(data_records), 1000):
                collection.insert_many(data_records[i:i+1000])
            logging.info(f"Inserted raw data for {ticker} into MongoDB.")
        # time.sleep(1)  # Rate limiting to avoid API blocks

logging.info("Data collection and saving to MongoDB complete.")
