from pyspark.sql import SparkSession
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from pymongo import MongoClient, errors
import pandas as pd
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

# Spark Session Setup
spark = SparkSession.builder \
    .appName("ClusteringDiversifiedAssets") \
    .getOrCreate()

# Load Data from MongoDB
logging.info("Loading data from MongoDB...")
data = pd.DataFrame(list(input_collection.find()))

# Drop MongoDB-specific ID column
if "_id" in data.columns:
    data.drop("_id", axis=1, inplace=True)

# Features to be used for clustering
required_features = ['Beta', 'Alpha', 'Dividend_Yield', '10-Day Volatility']

# Handle missing features and fill missing values
logging.info("Handling missing values...")
for feature in required_features:
    if feature not in data.columns:
        raise ValueError(f"Feature '{feature}' is missing from the dataset.")
    data[feature] = data[feature].fillna(data[feature].mean())

# Data Preprocessing
logging.info("Preprocessing data...")
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[required_features])

# Apply K-Means Clustering
logging.info("Applying K-Means Clustering...")
kmeans = KMeans(n_clusters=3, random_state=42)
kmeans.fit(data_scaled)
data["Cluster"] = kmeans.labels_

# Evaluate Clustering Performance
logging.info("Evaluating clustering performance...")
sil_score = silhouette_score(data_scaled, data["Cluster"])
logging.info(f"Silhouette Score: {sil_score:.2f}")

# Map Clusters to Risk Categories (0, 1, 2)
logging.info("Mapping clusters to risk categories...")
cluster_map = {0: "Low Risk", 1: "Medium Risk", 2: "High Risk"}
data["Risk Category"] = data["Cluster"].map(cluster_map)

# Step 5Save clustered data to MongoDB in batches
batch_size = 1000
clustered_records = data.to_dict("records")

logging.info(f"Saving clustered data to MongoDB collection: {output_collection_name}...")
try:
    for i in range(0, len(clustered_records), batch_size):
        logging.info(f"Inserting batch {i // batch_size + 1}...")
        output_collection.insert_many(clustered_records[i:i + batch_size])
except errors.AutoReconnect as e:
    logging.error(f"AutoReconnect error: {e}")
except errors.PyMongoError as e:
    logging.error(f"MongoDB error: {e}")

logging.info("Clustered data successfully saved to MongoDB!")
