import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import joblib  # For saving the model
from pymongo import MongoClient, errors
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
input_collection_name = "advanced_feature_engineered_data"

# Step 1: Load Data from MongoDB
def load_data_from_mongodb():
    logging.info("Loading data from MongoDB...")
    client = MongoClient(
        mongo_uri,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000,
        serverSelectionTimeoutMS=30000,
    )
    db = client[database_name]
    input_collection = db[input_collection_name]
    data = pd.DataFrame(list(input_collection.find()))

    # Drop MongoDB-specific ID column if present
    if "_id" in data.columns:
        data.drop("_id", axis=1, inplace=True)

    if "Daily Return" not in data.columns:
        logging.info("Calculating 'Daily Return' feature...")
        data["Daily Return"] = (data["Close"] - data["Open"]) / data["Open"]

    if "10-Day Volatility" not in data.columns:
        logging.info("Calculating '10-Day Volatility' feature...")
        data["10-Day Volatility"] = data["Daily Return"].rolling(window=10).std()

    logging.info("Data loaded successfully from MongoDB.")
    return data

# Step 2: Preprocess Data
def preprocess_data(data):
    logging.info("Preprocessing data...")
    required_features = ["Beta", "Alpha", "Dividend_Yield", "10-Day Volatility"]
    for feature in required_features:
        if feature not in data.columns:
            raise ValueError(f"Feature '{feature}' is missing from the dataset.")
        data[feature] = data[feature].fillna(data[feature].mean())
    return data, required_features

# Step 3: Train K-Means Clustering
def train_kmeans(data, features, n_clusters=3):
    logging.info("Scaling data...")
    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(data[features])
    
    logging.info("Training K-Means model...")
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    kmeans.fit(data_scaled)
    
    logging.info("Calculating Silhouette Score...")
    sil_score = silhouette_score(data_scaled, kmeans.labels_)
    logging.info(f"Silhouette Score: {sil_score:.2f}")
    
    # Add cluster labels to data for further use
    data["Cluster"] = kmeans.labels_
    return kmeans, scaler, data

# Step 4: Save Model and Scaler
def save_model(model, scaler, model_path="kmeans_model.pkl", scaler_path="scaler.pkl"):
    logging.info("Saving model and scaler...")
    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)
    logging.info("Model and scaler saved successfully!")

# Main Training Pipeline
if __name__ == "__main__":
    try:
        # Load and preprocess data
        data = load_data_from_mongodb()
        data, features = preprocess_data(data)
        
        # Train the model
        kmeans_model, scaler, clustered_data = train_kmeans(data, features)
        
        # Save the model and scaler for deployment
        save_model(kmeans_model, scaler)
        
        logging.info("Training pipeline completed successfully!")
    except Exception as e:
        logging.error(f"Error during training pipeline: {e}")
