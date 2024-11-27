import os
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timedelta

# Load environment variables from the .env file
load_dotenv()

# Function to check if returns exist in asset_metadata
def check_if_returns_exist(mongo_uri, database_name, collection_name, asset_name, field):
    """
    Check if a specific return field exists and contains a valid value for an asset.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the metadata collection.
        asset_name (str): Name of the asset (ticker).
        field (str): Field to check (e.g., 'YTD').

    Returns:
        bool: True if the field exists and contains valid numeric data, False otherwise.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Check if the field exists and is valid
    query = {"Ticker": asset_name, field: {"$exists": True, "$ne": None, "$not": {"$type": "string"}}}
    result = collection.find_one(query, {field: 1, "_id": 0})

    if result is None or pd.isna(result.get(field)):
        return False  # Field is missing or contains NaN

    return True

# Function to check if returns already exist in asset_metadata
def calculate_total_returns(hist_prices, today_date=None):
    if hist_prices.empty:
        print("Historical prices are empty. Cannot calculate returns.")
        return {"YTD": None, "1Y": None, "3Y": None, "5Y": None}

    today_date = datetime.strptime(today_date, "%Y-%m-%d") if today_date else datetime.today()
    start_of_year = datetime(today_date.year, 1, 1)

    # Initialize return values
    returns = {"YTD": None, "1Y": None, "3Y": None, "5Y": None}

    # Check YTD
    ytd_prices = hist_prices[hist_prices['Date'] >= start_of_year]
    if ytd_prices.empty:
        print("No prices found for YTD calculation.")
    else:
        ytd_start_close = ytd_prices.iloc[0]['Close']
        ytd_current_close = hist_prices.iloc[-1]['Close']
        returns["YTD"] = (ytd_current_close - ytd_start_close) / ytd_start_close * 100

    # Check 1Y, 3Y, 5Y
    for years, key in zip([1, 3, 5], ["1Y", "3Y", "5Y"]):
        start_date = today_date - timedelta(days=365 * years)
        past_prices = hist_prices[hist_prices['Date'] <= start_date]
        if past_prices.empty:
            print(f"No prices found for {key} calculation.")
        else:
            past_close = past_prices.iloc[-1]['Close']
            current_close = hist_prices.iloc[-1]['Close']
            returns[key] = (current_close - past_close) / past_close * 100

    return returns

# Function to fetch historical prices for a specific asset
def fetch_historical_prices(mongo_uri, database_name, collection_name, asset_name):
    """
    Fetch historical price data for a specific asset from MongoDB.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    cursor = collection.find({"Asset": asset_name}, {"_id": 0, "Date": 1, "Close": 1})
    data = list(cursor)
    if not data:
        print(f"No data found for asset: {asset_name}")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'], errors="coerce")  # Ensure 'Date' is a datetime object
    df['Close'] = pd.to_numeric(df['Close'].replace('[\$,]', '', regex=True), errors="coerce")  # Strip '$' and convert
    df.sort_values(by='Date', inplace=True)  # Sort by date
    return df

# Function to calculate total returns
def calculate_total_returns(hist_prices, today_date=None):
    """
    Calculate YTD, 1-year, 3-year, and 5-year total returns for an asset.

    Args:
        hist_prices (pd.DataFrame): DataFrame with historical prices (must have 'Date' and 'Close' columns).
        today_date (str): Optional. Specify today's date as 'YYYY-MM-DD' (defaults to current date).

    Returns:
        dict: Dictionary with YTD, 1-year, 3-year, and 5-year total returns.
    """
    if hist_prices.empty:
        return {"YTD": None, "1Y": None, "3Y": None, "5Y": None}

    today_date = datetime.strptime(today_date, "%Y-%m-%d") if today_date else datetime.today()
    start_of_year = datetime(today_date.year, 1, 1)

    # Initialize return values
    returns = {"YTD": None, "1Y": None, "3Y": None, "5Y": None}

    # Calculate YTD return
    ytd_prices = hist_prices[hist_prices['Date'] >= start_of_year]
    if not ytd_prices.empty:
        ytd_start_close = ytd_prices.iloc[0]['Close']
        ytd_current_close = hist_prices.iloc[-1]['Close']
        returns["YTD"] = (ytd_current_close - ytd_start_close) / ytd_start_close * 100

    # Calculate 1-year, 3-year, 5-year returns
    for years, key in zip([1, 3, 5], ["1Y", "3Y", "5Y"]):
        start_date = today_date - timedelta(days=365 * years)
        past_prices = hist_prices[hist_prices['Date'] <= start_date]
        if not past_prices.empty:
            past_close = past_prices.iloc[-1]['Close']
            current_close = hist_prices.iloc[-1]['Close']
            returns[key] = (current_close - past_close) / past_close * 100

    return returns

# Function to save returns in asset_metadata
def save_returns_to_metadata(mongo_uri, database_name, collection_name, asset_name, returns):
    """
    Save the calculated returns to the asset_metadata collection.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the metadata collection.
        asset_name (str): Name of the asset (ticker).
        returns (dict): Dictionary containing return metrics.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Update or insert the returns into the metadata collection
    update_result = collection.update_one(
        {"Ticker": asset_name},  # Match on Ticker
        {"$set": returns},       # Update return fields
        upsert=True              # Insert if not exists
    )
    if update_result.upserted_id:
        print(f"Inserted new document for {asset_name} in {collection_name}.")
    else:
        print(f"Updated document for {asset_name} in {collection_name}.")

# Main execution
if __name__ == "__main__":
    # Load MongoDB URI from environment variables
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in environment variables. Please check your .env file.")

    # Connect to MongoDB and fetch tickers from the historical_prices collection
    client = MongoClient(MONGO_URI)
    db = client["robo_advisor"]
    distinct_tickers = db["historical_prices"].distinct("Asset")

    print(f"Found {len(distinct_tickers)} tickers to process.")

    for ticker in distinct_tickers:
        print(f"Processing {ticker}...")

        # Check if the return data already exists and is valid
        if check_if_returns_exist(MONGO_URI, "robo_advisor", "asset_metadata", ticker, field="YTD"):
            print(f"YTD data already exists for {ticker}. Skipping calculation.")
            continue

        # Fetch historical prices
        hist_prices = fetch_historical_prices(MONGO_URI, "robo_advisor", "historical_prices", ticker)
        if hist_prices.empty:
            print(f"No historical prices found for {ticker}. Skipping...")
            continue

        # Calculate total returns
        returns = calculate_total_returns(hist_prices)
        print(f"Calculated returns for {ticker}: {returns}")

        # Save the calculated returns to the asset_metadata collection
        save_returns_to_metadata(MONGO_URI, "robo_advisor", "asset_metadata", ticker, returns)
        print(f"Updated returns for {ticker} in asset_metadata.")
