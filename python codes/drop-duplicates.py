from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Function to remove duplicates
def remove_duplicates(mongo_uri, database_name, collection_name, unique_field="Ticker"):
    """
    Remove duplicate documents from a MongoDB collection, keeping only the first document for each unique value.

    Args:
        mongo_uri (str): MongoDB URI for connection.
        database_name (str): Name of the database.
        collection_name (str): Name of the collection.
        unique_field (str): The field to identify unique documents.
    """
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Find all documents and group by the unique field
    pipeline = [
        {"$group": {
            "_id": f"${unique_field}",
            "ids": {"$push": "$_id"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}}  # Keep only duplicates
    ]

    duplicates = list(collection.aggregate(pipeline))
    print(f"Found {len(duplicates)} duplicate groups.")

    for duplicate in duplicates:
        ids = duplicate["ids"]
        # Keep the first document and delete the rest
        ids_to_delete = ids[1:]
        result = collection.delete_many({"_id": {"$in": ids_to_delete}})
        print(f"Deleted {result.deleted_count} duplicate documents for {duplicate['_id']}.")

    print("Duplicate removal complete.")

# Main execution
if __name__ == "__main__":
    # Retrieve MongoDB URI from .env file
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in environment variables. Please check your .env file.")

    DATABASE_NAME = "robo_advisor"
    COLLECTION_NAME = "asset_metadata"

    # Remove duplicates
    remove_duplicates(MONGO_URI, DATABASE_NAME, COLLECTION_NAME)
