import pandas as pd
from pymongo import MongoClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
input_collection_name = "asset_data"
output_collection_name = "processed_asset_data"

# MongoDB Client Setup
client = MongoClient(mongo_uri)
db = client[database_name]
input_collection = db[input_collection_name]
output_collection = db[output_collection_name]

# Load Data from MongoDB
logging.info("Fetching data from MongoDB...")
data = pd.DataFrame(list(input_collection.find()))

# Drop MongoDB-specific ID
if "_id" in data.columns:
    data.drop("_id", axis=1, inplace=True)

# Handle Missing Values
logging.info("Handling missing values...")
data.dropna(inplace=True)

# Ensure Datetime Format
logging.info("Converting 'Date' column to datetime format...")
data["Date"] = pd.to_datetime(data["Date"])

# Sort Data
logging.info("Sorting data by 'Date' for each Ticker...")
data.sort_values(by=["Ticker", "Date"], inplace=True)

# Keep Relevant Columns
logging.info("Filtering relevant columns...")
data = data[["Date", "Open", "High", "Low", "Close", "Volume", "Ticker", "Category"]]

# Save Processed Data to MongoDB
logging.info("Saving processed data back to MongoDB...")
processed_records = data.to_dict("records")
output_collection.insert_many(processed_records)

logging.info("Data preprocessing complete!")
