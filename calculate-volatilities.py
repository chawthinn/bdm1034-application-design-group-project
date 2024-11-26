import numpy as np
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Function to check if Volatility already exists in asset_metadata
def check_if_volatility_exists(mongo_uri, database_name, collection_name, asset_name, field="Volatility"):
    """
    Check if the volatility field exists for an asset in the metadata collection.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the metadata collection.
        asset_name (str): Name of the asset (ticker).
        field (str): The field to check (default is 'Volatility').

    Returns:
        bool: True if the field exists, False otherwise.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Query to check if the field exists for the asset
    existing_data = collection.find_one({"Ticker": asset_name}, {field: 1})
    return field in existing_data and existing_data[field] is not None if existing_data else False

def calculate_volatility(hist_prices, frequency="annual"):
    """
    Calculate volatility for a given asset's historical prices.
    
    Args:
        hist_prices (pd.DataFrame): DataFrame with 'Date' and 'Close' columns.
        frequency (str): Volatility time frame: 'daily', 'monthly', 'quarterly', or 'annual'.
    
    Returns:
        float: Volatility for the specified time frame.
    """
    if hist_prices.empty:
        return None

    # Calculate daily returns
    hist_prices['Daily_Return'] = hist_prices['Close'].pct_change()

    # Remove NaN values from returns
    daily_returns = hist_prices['Daily_Return'].dropna()

    # Standard deviation of daily returns (daily volatility)
    daily_volatility = daily_returns.std()

    # Adjust for different frequencies
    if frequency == "daily":
        return daily_volatility
    elif frequency == "monthly":
        return daily_volatility * np.sqrt(21)  # ~21 trading days in a month
    elif frequency == "quarterly":
        return daily_volatility * np.sqrt(63)  # ~63 trading days in a quarter
    elif frequency == "annual":
        return daily_volatility * np.sqrt(252)  # ~252 trading days in a year
    else:
        raise ValueError("Invalid frequency. Choose from 'daily', 'monthly', 'quarterly', or 'annual'.")

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
    df['Close'] = pd.to_numeric(df['Close'], errors='coerce')  # Convert 'Close' to numeric
    df.sort_values(by='Date', inplace=True)  # Sort by date
    return df

def save_volatility_to_metadata(mongo_uri, database_name, collection_name, asset_name, volatilities):
    """
    Save calculated volatilities to the asset_metadata collection.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the metadata collection.
        asset_name (str): Name of the asset (ticker).
        volatilities (dict): Dictionary containing volatilities for different time frames.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Update or insert the volatilities into the metadata collection
    update_result = collection.update_one(
        {"Ticker": asset_name},  # Match on Ticker
        {"$set": volatilities},  # Update volatility fields
        upsert=True  # Insert if not exists
    )
    if update_result.upserted_id:
        print(f"Inserted new document for {asset_name} with volatilities: {volatilities}")
    else:
        print(f"Updated document for {asset_name} with volatilities: {volatilities}")

if __name__ == "__main__":
    # Retrieve MongoDB URI from .env file
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in environment variables. Please check your .env file.")

    # Database and collection names
    DATABASE_NAME = "robo_advisor"
    HISTORICAL_PRICES_COLLECTION = "historical_prices"
    ASSET_METADATA_COLLECTION = "asset_metadata"

    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]

    # Fetch distinct tickers from the historical_prices collection
    distinct_tickers = db[HISTORICAL_PRICES_COLLECTION].distinct("Asset")

    for ticker in distinct_tickers:
        print(f"Processing {ticker}...")

        # Check if volatility metrics already exist
        if check_if_volatility_exists(MONGO_URI, DATABASE_NAME, ASSET_METADATA_COLLECTION, ticker):
            print(f"Volatility already exists for {ticker}. Skipping calculation.")
            continue

        # Fetch historical prices for the ticker
        hist_prices = fetch_historical_prices(MONGO_URI, DATABASE_NAME, HISTORICAL_PRICES_COLLECTION, ticker)

        if hist_prices.empty:
            print(f"No historical prices available for {ticker}. Skipping.")
            continue

        # Calculate volatilities for different time frames
        volatilities = {
            "Daily_Volatility": calculate_volatility(hist_prices, frequency="daily"),
            "Monthly_Volatility": calculate_volatility(hist_prices, frequency="monthly"),
            "Quarterly_Volatility": calculate_volatility(hist_prices, frequency="quarterly"),
            "Annual_Volatility": calculate_volatility(hist_prices, frequency="annual"),
        }

        # Save the calculated volatilities to the metadata collection
        save_volatility_to_metadata(MONGO_URI, DATABASE_NAME, ASSET_METADATA_COLLECTION, ticker, volatilities)

