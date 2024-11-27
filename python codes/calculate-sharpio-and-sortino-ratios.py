import numpy as np
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Function to check if Sharpe or Sortino Ratio already exists in asset_metadata
def check_if_ratios_exist(mongo_uri, database_name, collection_name, asset_name, fields=["Sharpe_Ratio", "Sortino_Ratio"]):
    """
    Check if specific ratio fields exist for an asset in the metadata collection.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the metadata collection.
        asset_name (str): Name of the asset (ticker).
        fields (list): List of fields to check (e.g., ['Sharpe_Ratio', 'Sortino_Ratio']).

    Returns:
        bool: True if all fields exist, False otherwise.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Query to check if fields exist for the asset
    existing_data = collection.find_one({"Ticker": asset_name}, {field: 1 for field in fields})
    return all(field in existing_data and existing_data[field] is not None for field in fields) if existing_data else False

# Function to calculate Sharpe and Sortino ratios
def calculate_ratios(hist_prices, risk_free_rate=0.02):
    """
    Calculate Sharpe and Sortino ratios for a given asset's historical prices.

    Args:
        hist_prices (pd.DataFrame): DataFrame with 'Date' and 'Close' columns.
        risk_free_rate (float): Annualized risk-free rate, default is 2% (0.02).

    Returns:
        dict: Sharpe and Sortino ratios.
    """
    if hist_prices.empty:
        return {"Sharpe_Ratio": None, "Sortino_Ratio": None}

    # Calculate daily returns
    hist_prices['Daily_Return'] = hist_prices['Close'].pct_change()

    # Remove NaN values from returns
    returns = hist_prices['Daily_Return'].dropna()

    # Calculate excess returns
    excess_returns = returns - (risk_free_rate / 252)  # Convert annual risk-free rate to daily

    # Sharpe Ratio
    sharpe_ratio = excess_returns.mean() / returns.std()

    # Sortino Ratio
    downside_returns = returns[returns < 0]
    sortino_ratio = excess_returns.mean() / downside_returns.std() if not downside_returns.empty else None

    return {
        "Sharpe_Ratio": sharpe_ratio,
        "Sortino_Ratio": sortino_ratio
    }

# Function to fetch historical prices for a specific asset
def fetch_historical_prices(mongo_uri, database_name, collection_name, asset_name):
    """
    Fetch historical price data for a specific asset from MongoDB.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the collection.
        asset_name (str): Name of the asset (ticker).

    Returns:
        pd.DataFrame: A DataFrame with historical price data for the asset.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Query to fetch data for the specific asset
    cursor = collection.find({"Asset": asset_name}, {"_id": 0, "Date": 1, "Close": 1})
    data = list(cursor)
    if not data:
        print(f"No data found for asset: {asset_name}")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'])  # Ensure 'Date' is a datetime object
    df['Close'] = pd.to_numeric(df['Close'].replace('[\$,]', '', regex=True), errors="coerce")  # Strip '$' and convert
    df.sort_values(by='Date', inplace=True)  # Sort by date
    return df

# Function to save Sharpe and Sortino ratios to MongoDB
def save_ratios_to_metadata(mongo_uri, database_name, collection_name, asset_name, ratios):
    """
    Save the calculated Sharpe and Sortino ratios to the asset_metadata collection.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the metadata collection.
        asset_name (str): Name of the asset (ticker).
        ratios (dict): Dictionary containing Sharpe and Sortino ratios.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Update or insert the ratios into the metadata collection
    update_result = collection.update_one(
        {"Ticker": asset_name},  # Match on Ticker
        {"$set": ratios},       # Update ratio fields
        upsert=True             # Insert if not exists
    )
    if update_result.upserted_id:
        print(f"Inserted new document for {asset_name} with Sharpe/Sortino ratios.")
    else:
        print(f"Updated document for {asset_name} with Sharpe/Sortino ratios.")

# Main execution
if __name__ == "__main__":
    # Retrieve MongoDB URI from .env file
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in environment variables. Please check your .env file.")

    # Fetch distinct tickers from the historical_prices collection
    client = MongoClient(MONGO_URI)
    db = client["robo_advisor"]
    distinct_tickers = db["historical_prices"].distinct("Asset")

    for ticker in distinct_tickers:
        print(f"Processing {ticker}...")

        # Check if Sharpe and Sortino ratios already exist
        if check_if_ratios_exist(MONGO_URI, "robo_advisor", "asset_metadata", ticker):
            print(f"Sharpe and Sortino ratios already exist for {ticker}. Skipping calculation.")
            continue

        # Fetch historical prices for the ticker
        hist_prices = fetch_historical_prices(MONGO_URI, "robo_advisor", "historical_prices", ticker)

        # Calculate Sharpe and Sortino ratios
        ratios = calculate_ratios(hist_prices)

        # Save the calculated ratios to the metadata collection
        save_ratios_to_metadata(MONGO_URI, "robo_advisor", "asset_metadata", ticker, ratios)
