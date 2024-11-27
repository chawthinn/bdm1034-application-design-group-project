import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Function to check if Risk-Adjusted Returns already exist in asset_metadata
def check_if_risk_adjusted_returns_exist(mongo_uri, database_name, collection_name, asset_name, fields=None):
    """
    Check if Risk-Adjusted Return fields exist for an asset in the metadata collection.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the metadata collection.
        asset_name (str): Name of the asset (ticker).
        fields (list): List of Risk-Adjusted Return fields to check (e.g., ['Risk_Adjusted_YTD', 'Risk_Adjusted_1Y']).

    Returns:
        bool: True if all fields exist and are not None, False otherwise.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    fields = fields or ["Risk_Adjusted_YTD", "Risk_Adjusted_1Y"]

    # Query to check if all specified fields exist and are not null
    existing_data = collection.find_one({"Ticker": asset_name}, {field: 1 for field in fields})
    return all(field in existing_data and existing_data[field] is not None for field in fields) if existing_data else False

# Function to fetch returns and volatility for a specific asset
def fetch_returns_and_volatility(mongo_uri, database_name, collection_name, asset_name):
    """
    Fetch return metrics and volatility for a specific asset from MongoDB.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the collection.
        asset_name (str): Name of the asset (ticker).

    Returns:
        dict: A dictionary containing return metrics and annual volatility.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Query to fetch returns and volatility
    asset_data = collection.find_one({"Ticker": asset_name}, {"YTD": 1, "1Y": 1, "Annual_Volatility": 1, "_id": 0})
    if not asset_data:
        print(f"No data found for asset: {asset_name}")
        return None

    return asset_data

# Function to calculate Risk-Adjusted Returns
def calculate_risk_adjusted_returns(returns_and_volatility):
    """
    Calculate Risk-Adjusted Returns (YTD and 1Y) for an asset.

    Args:
        returns_and_volatility (dict): Dictionary containing return metrics and annual volatility.

    Returns:
        dict: Dictionary containing Risk-Adjusted YTD and Risk-Adjusted 1Y returns.
    """
    if not returns_and_volatility or not returns_and_volatility.get("Annual_Volatility"):
        return {"Risk_Adjusted_YTD": None, "Risk_Adjusted_1Y": None}

    annual_volatility = returns_and_volatility["Annual_Volatility"]
    ytd_return = returns_and_volatility.get("YTD")
    one_year_return = returns_and_volatility.get("1Y")

    # Avoid division by zero
    if annual_volatility == 0:
        print("Annual Volatility is zero. Cannot calculate Risk-Adjusted Returns.")
        return {"Risk_Adjusted_YTD": None, "Risk_Adjusted_1Y": None}

    # Calculate Risk-Adjusted Returns
    risk_adjusted_ytd = ytd_return / annual_volatility if ytd_return is not None else None
    risk_adjusted_1y = one_year_return / annual_volatility if one_year_return is not None else None

    return {
        "Risk_Adjusted_YTD": risk_adjusted_ytd,
        "Risk_Adjusted_1Y": risk_adjusted_1y
    }

# Function to save Risk-Adjusted Returns to MongoDB
def save_risk_adjusted_returns_to_metadata(mongo_uri, database_name, collection_name, asset_name, risk_adjusted_returns):
    """
    Save Risk-Adjusted Returns to the asset_metadata collection.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the metadata collection.
        asset_name (str): Name of the asset (ticker).
        risk_adjusted_returns (dict): Dictionary containing Risk-Adjusted Returns.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Update or insert the Risk-Adjusted Returns
    update_result = collection.update_one(
        {"Ticker": asset_name},  # Match by Ticker
        {"$set": risk_adjusted_returns},  # Update return fields
        upsert=True  # Insert if not exists
    )
    if update_result.upserted_id:
        print(f"Inserted new document for {asset_name} with Risk-Adjusted Returns.")
    else:
        print(f"Updated document for {asset_name} with Risk-Adjusted Returns.")

# Main execution
if __name__ == "__main__":
    # Retrieve MongoDB URI from .env file
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in environment variables. Please check your .env file.")

    # MongoDB database and collection names
    DATABASE_NAME = "robo_advisor"
    ASSET_METADATA_COLLECTION = "asset_metadata"

    # Fetch distinct tickers from the asset_metadata collection
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    distinct_tickers = db[ASSET_METADATA_COLLECTION].distinct("Ticker")

    for ticker in distinct_tickers:
        print(f"Processing {ticker}...")

        # Check if Risk-Adjusted Returns already exist
        if check_if_risk_adjusted_returns_exist(MONGO_URI, DATABASE_NAME, ASSET_METADATA_COLLECTION, ticker):
            print(f"Risk-Adjusted Returns already exist for {ticker}. Skipping calculation.")
            continue

        # Fetch returns and volatility
        returns_and_volatility = fetch_returns_and_volatility(MONGO_URI, DATABASE_NAME, ASSET_METADATA_COLLECTION, ticker)
        if not returns_and_volatility:
            print(f"Skipping {ticker}: Missing required data.")
            continue

        # Calculate Risk-Adjusted Returns
        risk_adjusted_returns = calculate_risk_adjusted_returns(returns_and_volatility)

        # Save Risk-Adjusted Returns to MongoDB
        save_risk_adjusted_returns_to_metadata(MONGO_URI, DATABASE_NAME, ASSET_METADATA_COLLECTION, ticker, risk_adjusted_returns)
