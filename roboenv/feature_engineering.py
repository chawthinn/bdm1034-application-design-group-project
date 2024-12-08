import pandas as pd
import numpy as np
from pymongo import MongoClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
input_collection_name = "processed_asset_data"
output_collection_name = "feature_engineered_data"

# MongoDB Client Setup
client = MongoClient(mongo_uri)
db = client[database_name]
input_collection = db[input_collection_name]
output_collection = db[output_collection_name]

# Load Data from MongoDB
logging.info("Fetching processed data from MongoDB...")
data = pd.DataFrame(list(input_collection.find()))

# Drop MongoDB-specific ID
if "_id" in data.columns:
    data.drop("_id", axis=1, inplace=True)

# Feature Engineering Function
def engineer_features(df):
    # Sort by Ticker and Date (use copy to avoid warnings)
    df = df.sort_values(by=["Ticker", "Date"]).copy()

    # Calculate daily returns
    df["Daily Return"] = (df["Close"] - df["Open"]) / df["Open"]

    # Rolling averages and standard deviations
    df["10-Day SMA"] = df["Close"].rolling(window=10).mean()
    df["50-Day SMA"] = df["Close"].rolling(window=50).mean()
    df["200-Day SMA"] = df["Close"].rolling(window=200).mean()
    df["10-Day Volatility"] = df["Daily Return"].rolling(window=10).std()

    # Volume trends
    df["10-Day Avg Volume"] = df["Volume"].rolling(window=10).mean()
    df["50-Day Avg Volume"] = df["Volume"].rolling(window=50).mean()

    # Relative Strength Index (RSI)
    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))

    # Drop rows with NaN values (use inplace=False to return a new DataFrame)
    df = df.dropna()

    return df


# Apply feature engineering for each ticker
logging.info("Applying feature engineering...")
enhanced_data = []
for ticker in data["Ticker"].unique():
    logging.info(f"Processing features for {ticker}...")
    ticker_data = data[data["Ticker"] == ticker]
    enhanced_ticker_data = engineer_features(ticker_data)
    enhanced_data.append(enhanced_ticker_data)

# Combine all enhanced data
final_data = pd.concat(enhanced_data, ignore_index=True)

# Save Enhanced Data to MongoDB
logging.info("Saving feature-engineered data to MongoDB...")
final_records = final_data.to_dict("records")
output_collection.insert_many(final_records)

logging.info("Feature engineering complete!")
