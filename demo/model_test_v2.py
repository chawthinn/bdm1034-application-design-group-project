import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from pymongo import MongoClient, errors
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
input_collection_name = "advanced_feature_engineered_data"

# MongoDB Client Setup
client = MongoClient(
    mongo_uri,
    connectTimeoutMS=30000,
    socketTimeoutMS=60000,
    serverSelectionTimeoutMS=30000,
)
db = client[database_name]
input_collection = db[input_collection_name]

# Load Data from MongoDB
logging.info("Loading data from MongoDB...")
data = pd.DataFrame(list(input_collection.find()))

# Drop MongoDB-specific ID column
if "_id" in data.columns:
    data.drop("_id", axis=1, inplace=True)

if "Daily Return" not in data.columns:
    logging.info("Calculating 'Daily Return' feature...")
    data["Daily Return"] = (data["Close"] - data["Open"]) / data["Open"]

if "10-Day Volatility" not in data.columns:
    logging.info("Calculating '10-Day Volatility' feature...")
    data["10-Day Volatility"] = data["Daily Return"].rolling(window=10).std()

# Features to be used for clustering
required_features = ['Beta', 'Alpha', 'Dividend_Yield', '10-Day Volatility']

# Ensure required features are present
for feature in required_features:
    if feature not in data.columns:
        raise ValueError(f"Feature '{feature}' is missing from the dataset.")
    data[feature] = data[feature].fillna(data[feature].mean())

# Add Synthetic User Features
logging.info("Adding synthetic user data...")
user_age = 80
investment_amount = 100000
investment_duration = 20

data["User_Age"] = user_age
data["Investment_Amount"] = investment_amount
data["Investment_Duration"] = investment_duration

# Step 1: Data Preprocessing
logging.info("Preprocessing data...")
all_features = required_features + ["User_Age", "Investment_Amount", "Investment_Duration"]
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[all_features])

# Step 2: Apply K-Means Clustering
logging.info("Applying K-Means Clustering...")
kmeans = KMeans(n_clusters=3, random_state=42)
kmeans.fit(data_scaled)
data["Cluster"] = kmeans.labels_

# Step 3: Risk Score Calculation (Numerical Transformation)
logging.info("Calculating risk scores...")
data["Risk_Score"] = (
    np.maximum(0, 35 - data["User_Age"]) / 35 + np.minimum(data["Investment_Duration"], 5) / 5
)

# Map Risk Categories Based on Numerical Intervals
data["Risk_Category"] = pd.cut(
    data["Risk_Score"],
    bins=[0, 0.5, 1.0, 2.0],
    labels=["Low Risk", "Medium Risk", "High Risk"],
    include_lowest=True,
)

# Step 4: Create User-Specific Portfolios
logging.info("Creating user-specific portfolios...")

def create_portfolio(data, preferred_risk_category):
    # Filter data based on preferred risk category
    filtered_data = data[data["Risk_Category"].isin(preferred_risk_category)]
    # Group by category and sample a few stocks from each
    portfolio = filtered_data.groupby("Category").apply(
        lambda x: x.sample(n=min(5, len(x)), random_state=42)
    ).reset_index(drop=True)
    return portfolio

# Get Preferred Risk Categories
preferred_risk_category = data["Risk_Category"].unique()

# Create Portfolio for User
portfolio = create_portfolio(data, preferred_risk_category)

logging.info("Recommended Portfolio:")
print(portfolio[["Ticker", "Category", "Risk_Category"]])

import pickle

# Save the trained K-Means model
model_filename = "kmeans_model.pkl"
with open(model_filename, "wb") as file:
    pickle.dump(kmeans, file)

logging.info(f"Trained K-Means model saved to {model_filename}")
