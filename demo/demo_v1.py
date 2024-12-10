import streamlit as st
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# MongoDB Connection Details from .env
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = "robo_advisor"
COLLECTION_NAME = "clustered_stock_data"

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

# Streamlit App Layout
st.title("Robo Advisor: Clustered Stock Data Viewer")
st.subheader("Top 5 Data from Clustered Stock Data Collection")
if not df.empty:
    if "_id" in df.columns:
        df.drop(columns=["_id"], inplace=True)  # Drop MongoDB's default '_id' column
    st.dataframe(df)  # Display the DataFrame in a Streamlit table
else:
    st.error("No data available from MongoDB.")