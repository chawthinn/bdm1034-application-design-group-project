import streamlit as st
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import pickle
import logging

# Load the trained K-Means model
model_filename = "kmeans_model.pkl"
with open(model_filename, "rb") as file:
    kmeans = pickle.load(file)

logging.info("Trained K-Means model loaded successfully.")

# Streamlit App Title
st.title("Robo Advisor - User Portfolio Recommendation")

# User Inputs via Streamlit
st.header("User Input Features")

user_age = st.number_input("Enter User Age", min_value=18, max_value=100, value=80)
investment_amount = st.number_input("Enter Investment Amount", min_value=1000, step=1000, value=100000)
investment_duration = st.number_input("Enter Investment Duration (Years)", min_value=1, max_value=50, value=20)

st.write("User Input Summary:")
st.write(f"Age: {user_age}, Investment Amount: {investment_amount}, Investment Duration: {investment_duration}")

# Load a dummy dataset (you can replace this with real MongoDB data if needed)
# Assuming the original features ['Beta', 'Alpha', 'Dividend_Yield', '10-Day Volatility'] exist
data = pd.DataFrame({
    "Beta": np.random.rand(100),
    "Alpha": np.random.rand(100),
    "Dividend_Yield": np.random.rand(100),
    "10-Day Volatility": np.random.rand(100),
})

# Add synthetic user features
data["User_Age"] = user_age
data["Investment_Amount"] = investment_amount
data["Investment_Duration"] = investment_duration

# Features to be used for clustering
required_features = ['Beta', 'Alpha', 'Dividend_Yield', '10-Day Volatility', 
                     'User_Age', 'Investment_Amount', 'Investment_Duration']

# Step 1: Data Preprocessing
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[required_features])

# Step 2: Use the Loaded Model for Predictions
data["Cluster"] = kmeans.predict(data_scaled)

# Display the Clusters in the Streamlit App
st.header("Clustered Data")
st.write(data.head())

# Display Suggested Portfolio Based on Clustering (Optional Logic)
st.header("Suggested Portfolio (Example)")
st.write("Portfolio recommendations would go here based on clustering results.")
