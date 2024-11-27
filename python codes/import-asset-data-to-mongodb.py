import os
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from the .env file
load_dotenv()

# Function to fetch MongoDB tickers
def fetch_tickers_from_mongo(mongo_uri, database_name, collection_name, field="Asset"):
    """
    Fetch distinct tickers from a MongoDB collection.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the collection.
        field (str): Field to fetch distinct values from.

    Returns:
        list: A list of distinct tickers.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    
    # Fetch distinct tickers
    tickers = collection.distinct(field)
    print(f"Fetched {len(tickers)} tickers from MongoDB")
    return tickers

# Function to fetch asset data
def fetch_asset_data(tickers):
    """
    Fetch asset type, sector, and industry information for a list of tickers.

    Args:
        tickers (list): A list of ticker symbols.

    Returns:
        pd.DataFrame: A DataFrame with Ticker, Asset Type, Sector, Industry, and additional metadata.
    """
    data = []  # To store the results

    for ticker in tickers:
        try:
            # Download ticker info
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Fetch asset type
            asset_type = info.get("quoteType", "N/A")
            
            # Determine sector and industry based on asset type
            if asset_type == "ETF":
                sector = "N/A"
                industry = "N/A"
            else:
                sector = info.get("sector", "N/A")
                industry = info.get("industry", "N/A")
            
            # Append the details to the data list
            data.append({
                "Ticker": ticker,
                "Short Name": info.get("shortName", "N/A"),
                "Long Name": info.get("longName", "N/A"),
                "Asset_Type": asset_type,
                "Sector": sector,
                "Industry": industry,
                "Exchange": info.get("exchange", "N/A"),
                "Currency": info.get("currency", "N/A"),
                "Description": info.get("longBusinessSummary", "N/A")
            })
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            # Append error details for this ticker
            data.append({
                "Ticker": ticker,
                "Short Name": "Error",
                "Long Name": "Error",
                "Asset_Type": "Error",
                "Sector": "Error",
                "Industry": "Error",
                "Exchange": "Error",
                "Currency": "Error",
                "Description": "Error"
            })
    
    # Convert to DataFrame
    return pd.DataFrame(data)

# Function to save asset data to a new MongoDB collection
def save_to_mongo(mongo_uri, database_name, collection_name, data_df):
    """
    Save a DataFrame to a MongoDB collection.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the target collection.
        data_df (pd.DataFrame): DataFrame to be saved to MongoDB.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    
    # Convert DataFrame to dictionary format for MongoDB
    data_dict = data_df.to_dict("records")
    
    # Insert data into the collection
    result = collection.insert_many(data_dict)
    print(f"Inserted {len(result.inserted_ids)} documents into '{collection_name}' collection.")


# Main execution
if __name__ == "__main__":
    # Retrieve MongoDB URI from .env file
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in environment variables. Please check your .env file.")
    DATABASE_NAME = "robo_advisor"
    COLLECTION_NAME = "historical_prices"
    NEW_COLLECTION_NAME = "asset_metadata"

    # Fetch tickers from MongoDB
    tickers = fetch_tickers_from_mongo(MONGO_URI, DATABASE_NAME, COLLECTION_NAME)

    # Fetch asset data for tickers
    asset_data_df = fetch_asset_data(tickers)

    # Save the processed data to a new MongoDB collection
    save_to_mongo(MONGO_URI, DATABASE_NAME, NEW_COLLECTION_NAME, asset_data_df)

    # Display the DataFrame
    print(asset_data_df)