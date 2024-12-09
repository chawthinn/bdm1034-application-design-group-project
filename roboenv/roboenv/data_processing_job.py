import pandas as pd
from pymongo import MongoClient
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
input_collection_name = "enhanced_asset_data"
output_collection_name = "processed_asset_data"

# MongoDB Client Setup
client = MongoClient(mongo_uri)
db = client[database_name]
input_collection = db[input_collection_name]
output_collection = db[output_collection_name]

try:
    # Step 1: Load Data from MongoDB
    logging.info("Fetching data from MongoDB...")
    data = pd.DataFrame(list(input_collection.find()))
    
    # Drop MongoDB-specific ID column
    if "_id" in data.columns:
        data.drop("_id", axis=1, inplace=True)
    
    # Step 2: Handle Missing Values
    logging.info("Handling missing values...")
    data.dropna(inplace=True)
    
    # Step 3: Ensure Datetime Format
    logging.info("Converting 'Date' column to datetime format...")
    data["Date"] = pd.to_datetime(data["Date"])
    
    # Step 4: Sort Data
    logging.info("Sorting data by 'Date' for each Ticker...")
    data.sort_values(by=["Ticker", "Date"], inplace=True)
    
    # Step 5: Keep Relevant Columns
    logging.info("Filtering relevant columns...")
    relevant_columns = [
        "Date", "Open", "High", "Low", "Close", "Volume", 
        "Ticker", "Category", "Momentum", "Beta", "Alpha", 
        "PE_Ratio", "Dividend_Yield"
    ]
    data = data[relevant_columns]
    
    # Step 6: Save Processed Data Back to MongoDB
    logging.info("Saving processed data back to MongoDB...")
    processed_records = data.to_dict("records")
    output_collection.delete_many({})  # Clear the output collection before inserting new data
    output_collection.insert_many(processed_records)
    
    logging.info("Data preprocessing complete!")
except Exception as e:
    logging.error(f"An error occurred: {e}")
finally:
    # Close MongoDB client
    client.close()
