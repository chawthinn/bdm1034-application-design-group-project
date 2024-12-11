import os
os.system("pip install pymongo yfinance pandas")
import yfinance as yf
import pandas as pd
import logging
from pymongo import MongoClient

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
collection_name = "enhanced_asset_data"

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

# Market index for beta and alpha calculation
market_index_ticker = "SPY"

# Function to calculate additional features
def calculate_features(data, market_data):
    try:
        # Ensure data is aligned with market data for beta and alpha
        data = data.join(market_data["Close"], rsuffix="_market", how="inner")
        
        # Momentum: Percentage change over the last 14 days
        data["Momentum"] = data["Close"].pct_change(periods=14)
        
        # Beta: Covariance of asset and market returns / Variance of market returns
        data["Return"] = data["Close"].pct_change()
        data["Market_Return"] = data["Close_market"].pct_change()
        covariance = data[["Return", "Market_Return"]].cov().iloc[0, 1]
        variance = data["Market_Return"].var()
        data["Beta"] = covariance / variance
        
        # Alpha: Excess return over the market
        risk_free_rate = 0.01 / 252  # Example risk-free rate (annualized to daily)
        data["Alpha"] = data["Return"] - (risk_free_rate + data["Beta"] * (data["Market_Return"] - risk_free_rate))
        
        # Dividend Yield: Dividends / Close Price
        data["Dividend_Yield"] = data["Dividends"] / data["Close"]
        
        # P/E Ratio: Close Price / EPS
        data["PE_Ratio"] = data["Close"] / data["EPS"]
        
        return data
    except Exception as e:
        logging.error(f"Error in feature calculation: {e}")
        return None

# Function to fetch historical data for a given ticker
def fetch_data(ticker, start="2000-01-01", end="2023-12-31", interval="1d"):
    try:
        logging.info(f"Fetching raw data for {ticker}...")
        # Fetch adjusted data including dividends
        ticker_data = yf.Ticker(ticker)
        data = ticker_data.history(start=start, end=end, interval=interval)
        
        if data.empty:
            logging.warning(f"No data found for {ticker}. Skipping...")
            return None
        
        # Include relevant columns
        data = data[["Open", "High", "Low", "Close", "Volume", "Dividends"]].copy()
        data["EPS"] = ticker_data.info.get("trailingEps", None)  # Fetch EPS
        data["Ticker"] = ticker  # Add ticker symbol
        data.reset_index(inplace=True)  # Reset index for MongoDB insertion
        return data
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return None

# Fetch market index data
logging.info("Fetching market index data...")
market_data = fetch_data(market_index_ticker)
if market_data is not None:
    market_data.set_index("Date", inplace=True)  # Prepare market data for joining

# Fetch data for all assets and save to MongoDB
for category, tickers in assets.items():
    for ticker in tickers:
        data = fetch_data(ticker)
        if data is not None and market_data is not None:
            data.set_index("Date", inplace=True)  # Align on Date for calculations
            final_data = calculate_features(data, market_data)
            
            if final_data is not None:
                final_data.reset_index(inplace=True)  # Reset index for MongoDB insertion
                final_data["Category"] = category  # Add category for each asset
                final_data.dropna(inplace=True)  # Drop missing values
                data_records = final_data.to_dict("records")  # Convert to dictionary format
                
                # Insert in batches to avoid overloading
                for i in range(0, len(data_records), 1000):
                    collection.insert_many(data_records[i:i+1000])
                logging.info(f"Inserted enhanced data for {ticker} into MongoDB.")
        else:
            logging.warning(f"Skipping {ticker} due to missing market data.")

logging.info("Data collection and saving to MongoDB complete.")
