import pandas as pd
import numpy as np
from pymongo import MongoClient, errors
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = (
    "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/"
    "?retryWrites=true&w=majority&appName=Cluster0"
)
database_name = "robo_advisor"
input_collection_name = "processed_asset_data"
output_collection_name = "advanced_feature_engineered_data"

# MongoDB Client Setup with increased timeouts
client = MongoClient(
    mongo_uri,
    connectTimeoutMS=30000,
    socketTimeoutMS=60000,
    serverSelectionTimeoutMS=30000,
)
db = client[database_name]
input_collection = db[input_collection_name]
output_collection = db[output_collection_name]

# Load Data from MongoDB
logging.info("Fetching processed data from MongoDB...")
data = pd.DataFrame(list(input_collection.find()))

# Drop MongoDB-specific ID
if "_id" in data.columns:
    data.drop("_id", axis=1, inplace=True)

# Ensure proper data types for datetime
data["Date"] = pd.to_datetime(data["Date"])

# Feature Engineering Function
def engineer_advanced_features(df):
    # Sort by Ticker and Date
    df = df.sort_values(by=["Ticker", "Date"]).copy()

    # Log Returns
    df["Log_Returns"] = np.log(df["Close"] / df["Close"].shift(1))

    # Moving Averages
    df["20-Day SMA"] = df["Close"].rolling(window=20).mean()
    df["50-Day SMA"] = df["Close"].rolling(window=50).mean()
    df["100-Day SMA"] = df["Close"].rolling(window=100).mean()

    # Exponential Moving Averages (EMA)
    df["20-Day EMA"] = df["Close"].ewm(span=20, adjust=False).mean()
    df["50-Day EMA"] = df["Close"].ewm(span=50, adjust=False).mean()

    # Bollinger Bands
    rolling_mean = df["Close"].rolling(window=20).mean()
    rolling_std = df["Close"].rolling(window=20).std()
    df["Upper_Band"] = rolling_mean + (2 * rolling_std)
    df["Lower_Band"] = rolling_mean - (2 * rolling_std)

    # Price-to-Volume Ratio
    df["Price_Volume_Ratio"] = df["Close"] / df["Volume"]

    # Historical Volatility
    df["Historical_Volatility"] = df["Log_Returns"].rolling(window=20).std()

    # Price Change
    df["Price_Change"] = df["Close"] - df["Open"]

    # Momentum Indicators
    df["Momentum_10"] = df["Close"].diff(periods=10)
    df["Momentum_20"] = df["Close"].diff(periods=20)

    # Drop rows with NaN values
    df = df.dropna()

    return df

# Apply feature engineering for each ticker
logging.info("Applying advanced feature engineering...")
advanced_data = []
for ticker in data["Ticker"].unique():
    logging.info(f"Processing features for {ticker}...")
    ticker_data = data[data["Ticker"] == ticker]
    enhanced_ticker_data = engineer_advanced_features(ticker_data)
    advanced_data.append(enhanced_ticker_data)

# Combine all enhanced data
final_data = pd.concat(advanced_data, ignore_index=True)

# Save Enhanced Data to MongoDB in smaller batches
batch_size = 1000
final_records = final_data.to_dict("records")

logging.info("Saving advanced feature-engineered data to MongoDB...")
try:
    for i in range(0, len(final_records), batch_size):
        logging.info(f"Inserting batch {i // batch_size + 1}...")
        output_collection.insert_many(final_records[i : i + batch_size])
except errors.AutoReconnect as e:
    logging.error(f"AutoReconnect error: {e}")
except errors.PyMongoError as e:
    logging.error(f"MongoDB error: {e}")

logging.info("Advanced feature engineering complete!")
