from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone
import os

# Load environment variables from the .env file
load_dotenv()

# Function to check if a date is in mm/dd/yyyy format
def is_mm_dd_yyyy(date_string):
    """
    Check if a date is in mm/dd/yyyy format.

    Args:
        date_string (str): Date string to check.

    Returns:
        bool: True if the date is in mm/dd/yyyy format, False otherwise.
    """
    try:
        datetime.strptime(date_string, "%m/%d/%Y")
        return True
    except ValueError:
        return False

# Function to standardize dates for a specific ticker
def standardize_dates_for_ticker(mongo_uri, database_name, collection_name, ticker):
    """
    Standardize the Date field for all documents of a specific ticker.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the collection.
        ticker (str): Ticker name to process.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Fetch documents for the ticker
    cursor = collection.find({"Asset": ticker}, {"_id": 1, "Date": 1})
    documents = list(cursor)  # Convert cursor to a list to enable counting
    total_documents = len(documents)
    updates = 0

    for idx, document in enumerate(documents, start=1):
        doc_id = document["_id"]
        raw_date = document.get("Date")

        if is_mm_dd_yyyy(raw_date):  # Process only if the date is in mm/dd/yyyy format
            try:
                # Convert Date to standard format
                standardized_date = datetime.strptime(raw_date, "%m/%d/%Y").strftime("%Y-%m-%d")

                # Current timestamp with UTC
                now = datetime.now(timezone.utc)
                last_updated = now.strftime("%Y-%m-%d")
                last_updated_timestamp = now

                # Update the document
                collection.update_one(
                    {"_id": doc_id},
                    {
                        "$set": {
                            "Date": standardized_date,
                            "last_updated": last_updated,
                            "last_updated_timestamp": last_updated_timestamp,
                        }
                    }
                )
                updates += 1

                # Display progress
                print(f"Updated document {idx}/{total_documents} for ticker {ticker} (_id: {doc_id}).")

            except Exception as e:
                print(f"Error processing document with _id {doc_id}: {e}")

    if updates > 0:
        print(f"Completed standardizing {updates}/{total_documents} dates for ticker {ticker}.")
    else:
        print(f"No updates required for ticker {ticker}.")

# Main execution
if __name__ == "__main__":
    # Retrieve MongoDB URI from .env file
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in environment variables. Please check your .env file.")

    # Database and collection names
    DATABASE_NAME = "robo_advisor"
    COLLECTION_NAME = "historical_prices"

    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]

    # Get all distinct tickers
    distinct_tickers = db[COLLECTION_NAME].distinct("Asset")

    for ticker in distinct_tickers:
        # Check if any document for this ticker has a wrong date format
        has_wrong_format = db[COLLECTION_NAME].find_one(
            {"Asset": ticker, "Date": {"$regex": r"^\d{2}/\d{2}/\d{4}$"}}, {"_id": 1}
        )

        if has_wrong_format:  # If at least one document has mm/dd/yyyy format, process the ticker
            print(f"Processing ticker {ticker} with incorrect date format...")
            standardize_dates_for_ticker(MONGO_URI, DATABASE_NAME, COLLECTION_NAME, ticker)
