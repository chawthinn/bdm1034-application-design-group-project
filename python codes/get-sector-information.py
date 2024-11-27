import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["robo_advisor"]  # Use the "robo_advisor" database
collection = db["asset_metadata"]  # Use the "asset_metadata" collection

# Fetch the list of tickers from the database
tickers = collection.distinct("Ticker")  # Get unique tickers from the collection

# Iterate over tickers and update missing data
data = []  # For tracking updates or errors
for ticker in tickers:
    try:
        # Check if Short Name, Long Name, or Asset_Type is missing
        document = collection.find_one({"Ticker": ticker}, {"Short Name": 1, "Long Name": 1, "Asset_Type": 1, "_id": 1})
        
        if not document:
            print(f"No document found for {ticker}, skipping...")
            continue

        # If any of these fields are missing or "N/A", fetch the data
        if (
            document.get("Short Name") in [None, "N/A"] or
            document.get("Long Name") in [None, "N/A"] or
            document.get("Asset_Type") in [None, "N/A"]
        ):
            # Fetch data using yfinance
            stock = yf.Ticker(ticker)
            info = stock.info

            # Prepare the fields to update
            update_fields = {
                "Short Name": info.get("shortName", "N/A"),
                "Long Name": info.get("longName", "N/A"),
                "Asset_Type": info.get("quoteType", "N/A"),
                "Sector": info.get("sector", "N/A"),
                "Industry": info.get("industry", "N/A"),
                "Exchange": info.get("exchange", "N/A"),
                "Currency": info.get("currency", "N/A"),
                "Description": info.get("longBusinessSummary", "N/A"),
            }

            # Update the MongoDB document
            collection.update_one({"_id": document["_id"]}, {"$set": update_fields})
            print(f"Updated document for {ticker}: {update_fields}")
        else:
            print(f"All required fields already exist for {ticker}. Skipping...")
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        data.append({"Ticker": ticker, "Error": str(e)})

# Convert the data into a Pandas DataFrame for logs or debugging
if data:
    error_df = pd.DataFrame(data)
    print("Errors or updates encountered:")
    print(error_df)
else:
    print("All tickers processed successfully.")
