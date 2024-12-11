import streamlit as st
import pandas as pd
import numpy as np
import pickle
from sklearn.metrics import silhouette_score
from pymongo import MongoClient

# Load pre-trained models
with open('kmeans_model.pkl', 'rb') as f:
    kmeans = pickle.load(f)

with open('scaler.pkl', 'rb') as f:
    scaler = pickle.load(f)

# MongoDB connection details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
input_collection_name = "advanced_feature_engineered_data"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[database_name]
input_collection = db[input_collection_name]

# Streamlit App Title
st.title("Stock Clustering and Investment Advisor")

# User Inputs
age = st.number_input("Enter your age:", min_value=18, max_value=100, step=1)
investment_amount = st.number_input("Enter your investment amount ($):", min_value=1000, step=100)
investment_duration = st.number_input("Enter investment duration (years):", min_value=1, max_value=50, step=1)

if st.button("Analyze and Cluster"):
    # Load data from MongoDB
    data = pd.DataFrame(list(input_collection.find()))

    # Drop MongoDB-specific ID column
    if "_id" in data.columns:
        data.drop("_id", axis=1, inplace=True)

    # Required Features
    required_features = ['Beta', 'Alpha', 'Dividend_Yield', '10-Day Volatility']

    # Calculate missing features
    if "Daily Return" not in data.columns:
        st.info("Calculating 'Daily Return' feature...")
        data["Daily Return"] = (data["Close"] - data["Open"]) / data["Open"]

    if "10-Day Volatility" not in data.columns:
        st.info("Calculating '10-Day Volatility' feature...")
        data["10-Day Volatility"] = data["Daily Return"].rolling(window=10).std()

    # Check and preprocess data
    for feature in required_features:
        if feature not in data.columns:
            st.error(f"Missing feature: {feature}")
            st.stop()

    data = data[required_features].fillna(data.mean())

    # Scale data
    data_scaled = scaler.transform(data)

    # Predict clusters
    data['Cluster'] = kmeans.predict(data_scaled)

    # Calculate Silhouette Score
    sil_score = silhouette_score(data_scaled, data['Cluster'])

    # Display Results
    st.subheader("Clustering Results")
    st.write(f"Silhouette Score: {sil_score:.2f}")
    st.write(data)

    # Tailored Recommendation
    st.subheader("Investment Recommendation")
    if age < 35 and investment_duration > 10:
        st.write("You are in a prime investment phase. Consider aggressive portfolios (e.g., Cluster 2).")
    elif 35 <= age <= 50:
        st.write("A balanced portfolio might suit you better (e.g., Cluster 1).")
    else:
        st.write("Consider conservative options for steady growth (e.g., Cluster 0).")