from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
output_collection_name = "clustered_stock_data"

# MongoDB Client Setup
client = MongoClient(
    mongo_uri,
    connectTimeoutMS=30000,
    socketTimeoutMS=60000,
    serverSelectionTimeoutMS=30000,
)
db = client[database_name]
input_collection = db[input_collection_name]
output_collection = db[output_collection_name]

# Load Data from MongoDB
logging.info("Loading data from MongoDB...")
data = pd.DataFrame(list(input_collection.find()))

# Drop MongoDB-specific ID column
if "_id" in data.columns:
    data.drop("_id", axis=1, inplace=True)

# Ensure required features are present
required_features = ['Beta', 'Alpha', 'Dividend_Yield', '10-Day Volatility']

# Calculate missing features
if "Daily Return" not in data.columns:
    logging.info("Calculating 'Daily Return' feature...")
    data["Daily Return"] = (data["Close"] - data["Open"]) / data["Open"]

if "10-Day Volatility" not in data.columns:
    logging.info("Calculating '10-Day Volatility' feature...")
    data["10-Day Volatility"] = data["Daily Return"].rolling(window=10).std()

if "50-Day Avg Volume" not in data.columns:
    logging.info("Calculating '50-Day Avg Volume' feature...")
    if "Volume" in data.columns:
        data["50-Day Avg Volume"] = data["Volume"].rolling(window=50).mean()
    else:
        raise ValueError("Feature '50-Day Avg Volume' requires 'Volume' column, which is missing.")

if "RSI" not in data.columns:
    logging.info("Calculating 'RSI' feature...")
    delta = data["Close"].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
    rs = gain / loss
    data["RSI"] = 100 - (100 / (1 + rs))

# Handle missing features and fill missing values
logging.info("Handling missing values...")
for feature in required_features:
    if feature not in data.columns:
        raise ValueError(f"Feature '{feature}' is missing from the dataset.")
    # Fill missing values with the mean for numerical features
    data[feature] = data[feature].fillna(data[feature].mean())

# Step 1: Data Preprocessing
logging.info("Preprocessing data...")
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[required_features])

# Step 2: Apply K-Means Clustering for diversified assets
logging.info("Applying K-Means Clustering...")
kmeans = KMeans(n_clusters=3, random_state=42)  # 3 clusters for diversified assets
kmeans.fit(data_scaled)
data['Cluster'] = kmeans.labels_

# Step 3: Evaluate Clustering Performance
logging.info("Evaluating clustering performance...")
sil_score = silhouette_score(data_scaled, data['Cluster'])
logging.info(f"Silhouette Score: {sil_score:.2f}")

# Return the Silhouette Score
print(f"Silhouette Score: {sil_score:.2f}")
