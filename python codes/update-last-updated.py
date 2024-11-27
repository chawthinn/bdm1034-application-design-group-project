from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# MongoDB connection setup
client = MongoClient(MONGO_URI)
db = client["robo_advisor"]  # Use the "robo_advisor" database
collection = db["historical_prices"]  # Use the "market_data" collection

# Current date and time
current_date = datetime.now()

# Update documents where 'last_updated' is missing
result_last_updated = collection.update_many(
    {"last_updated": {"$exists": False}},  # Filter: Documents missing 'last_updated'
    {"$set": {"last_updated": current_date}}  # Update: Set 'last_updated' to current date
)

# Update documents where 'last_updated_timestamp' is missing
result_last_updated_timestamp = collection.update_many(
    {"last_updated_timestamp": {"$exists": False}},  # Filter: Documents missing 'last_updated_timestamp'
    {"$set": {"last_updated_timestamp": current_date}}  # Update: Set 'last_updated_timestamp' to current date
)

# Print update results
print(f"'last_updated' field updated in {result_last_updated.modified_count} documents.")
print(f"'last_updated_timestamp' field updated in {result_last_updated_timestamp.modified_count} documents.")
