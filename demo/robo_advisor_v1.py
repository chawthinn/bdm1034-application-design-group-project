import streamlit as st
import pandas as pd
import os
from pymongo import MongoClient

# Fetch MongoDB connection details from environment variables
MONGO_URI = os.getenv("MONGO_URI")  # Ensure MONGO_URI is set in the environment
DATABASE_NAME = os.getenv("DATABASE_NAME", "robo_advisor")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "clustered_stock_data")

@st.cache_resource
def fetch_data_from_mongodb():
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Fetch top 5 data from MongoDB
        cursor = collection.find().limit(5)  # Fetch the first 5 documents
        data = list(cursor)  # Convert cursor to a list
        return pd.DataFrame(data)  # Convert to pandas DataFrame
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

# Fetch Data
df = fetch_data_from_mongodb()

# App Title
st.title('Personalized Robo-Advisor: Portfolio Suggestion')

# Show Top 5 Data
st.subheader('Top 5 Data from Clustered Stock Data')
if not df.empty:
    if "_id" in df.columns:
        df.drop(columns=["_id"], inplace=True)  # Drop the MongoDB-specific '_id' column
    st.dataframe(df)
else:
    st.error("No data available from MongoDB.")

st.write("App is successfully using environment variables for database connection.")
