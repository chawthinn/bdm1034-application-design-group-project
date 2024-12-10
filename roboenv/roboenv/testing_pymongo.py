from pymongo import MongoClient
import pandas as pd

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/robo_advisor?retryWrites=true&w=majority"
database_name = "robo_advisor"
collection_name = "enhanced_asset_data"

try:
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Fetch data from MongoDB
    cursor = collection.find()  # Fetch all documents in the collection
    data = list(cursor)  # Convert cursor to a list

    if data:
        # Convert to pandas DataFrame for easier manipulation
        df = pd.DataFrame(data)
        print("Connection successful! Sample data:")
        print(df.head())  # Display first 5 rows of the data
    else:
        print("Connection successful, but the collection is empty.")
except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")
finally:
    # Close the MongoDB connection
    client.close()

# Export to CSV
output_file = "enhanced_asset_data.csv"
df.to_csv(output_file, index=False)  # `index=False` prevents writing the DataFrame index to the file
print(f"Data successfully exported to {output_file}")
