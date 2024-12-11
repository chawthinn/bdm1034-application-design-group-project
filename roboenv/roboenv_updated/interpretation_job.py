import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from pymongo import MongoClient, errors
import shap
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

# Check and calculate missing features
if "Daily Return" not in data.columns:
    logging.info("Calculating 'Daily Return' feature...")
    data["Daily Return"] = (data["Close"] - data["Open"]) / data["Open"]

if "10-Day Volatility" not in data.columns:
    logging.info("Calculating '10-Day Volatility' feature...")
    data["10-Day Volatility"] = data["Daily Return"].rolling(window=10).std()
    data["10-Day Volatility"] = data["10-Day Volatility"].fillna(data["10-Day Volatility"].mean())

# Features to be used for clustering
required_features = ["Beta", "Alpha", "Dividend_Yield", "10-Day Volatility"]

# Ensure required features are present
logging.info("Validating required features...")
for feature in required_features:
    if feature not in data.columns:
        raise ValueError(f"Feature '{feature}' is missing from the dataset.")
    data[feature] = data[feature].fillna(data[feature].mean())

# Step 1: Preprocess Data
logging.info("Preprocessing data...")
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[required_features])

# Step 2: Load Trained K-Means Model
logging.info("Loading trained K-Means model...")
from sklearn.externals import joblib
kmeans_model = joblib.load("kmeans_model.pkl")
data["Cluster"] = kmeans_model.predict(data_scaled)

# Step 3: Explain Clustering with SHAP
logging.info("Explaining clustering with SHAP...")

# Create a SHAP Explainer
explainer = shap.KernelExplainer(kmeans_model.predict, data_scaled)

# Compute SHAP Values
logging.info("Calculating SHAP values...")
shap_values = explainer.shap_values(data_scaled)

# Create a DataFrame for SHAP values
shap_df = pd.DataFrame(shap_values, columns=required_features)

# Step 4: Aggregate SHAP Values by Cluster
logging.info("Aggregating SHAP values by cluster...")
shap_df["Cluster"] = data["Cluster"]
shap_summary = shap_df.groupby("Cluster").mean()

# Output SHAP summary
logging.info("SHAP Summary (Average Contribution by Feature for Each Cluster):")
print(shap_summary)

# Step 5: Visualize SHAP Explanations
logging.info("Visualizing SHAP explanations...")
shap.summary_plot(shap_values, data_scaled, feature_names=required_features)

# Step 6: Cluster Insights
logging.info("Cluster Insights:")
for cluster_id in range(kmeans_model.n_clusters):
    logging.info(f"Cluster {cluster_id} Characteristics:")
    logging.info(shap_summary.loc[cluster_id])

# Save SHAP results to MongoDB (Optional)
interpretation_collection_name = "interpretation_results"
interpretation_collection = db[interpretation_collection_name]

shap_results = shap_summary.reset_index().to_dict("records")
try:
    logging.info("Saving interpretation results to MongoDB...")
    interpretation_collection.insert_many(shap_results)
except errors.PyMongoError as e:
    logging.error(f"Error saving interpretation results: {e}")

logging.info("Interpretation pipeline completed successfully.")
