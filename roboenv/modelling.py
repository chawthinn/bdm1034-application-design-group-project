import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
from pymongo import MongoClient

# MongoDB Connection
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
collection_name = "feature_engineered_data"

# Load Data from MongoDB
client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

print("Loading data from MongoDB...")
data = pd.DataFrame(list(collection.find()))

# Drop MongoDB-specific ID column
if "_id" in data.columns:
    data.drop("_id", axis=1, inplace=True)

# Features to be used for clustering
features = ["Daily Return", "50-Day SMA", "10-Day Volatility", "50-Day Avg Volume", "RSI"]

# Step 1: Data Preprocessing
print("Preprocessing data...")
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[features])

# Step 2: Determine the optimal number of clusters using the Elbow Method
print("Determining optimal number of clusters...")
inertia = []
K = range(2, 10)  # Testing for k from 2 to 9
for k in K:
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(data_scaled)
    inertia.append(kmeans.inertia_)

# Plot the Elbow Curve
plt.figure(figsize=(8, 5))
plt.plot(K, inertia, marker='o')
plt.title('Elbow Method for Optimal k')
plt.xlabel('Number of Clusters (k)')
plt.ylabel('Inertia')
plt.grid(True)
plt.show()

# Step 3: Apply K-Means Clustering
print("Applying K-Means Clustering...")
kmeans = KMeans(n_clusters=3, random_state=42)
kmeans.fit(data_scaled)
data['Cluster'] = kmeans.labels_

# Step 4: Evaluate Clustering Performance
sil_score = silhouette_score(data_scaled, data['Cluster'])
print(f"Silhouette Score: {sil_score:.2f}")

# Step 5: Visualize Clusters
# from sklearn.decomposition import PCA
# pca = PCA(n_components=2)
# data_pca = pca.fit_transform(data_scaled)
# data['PCA1'] = data_pca[:, 0]
# data['PCA2'] = data_pca[:, 1]

# plt.figure(figsize=(8, 6))
# for cluster in range(3):
#     cluster_data = data[data['Cluster'] == cluster]
#     plt.scatter(cluster_data['PCA1'], cluster_data['PCA2'], label=f'Cluster {cluster}')
# plt.title('Clusters Visualization (PCA)')
# plt.xlabel('PCA1')
# plt.ylabel('PCA2')
# plt.legend()
# plt.grid(True)
# plt.show()

# Step 6: Map Clusters to Risk Categories
print("Mapping clusters to risk categories...")
cluster_map = {
    0: 'Low Risk',
    1: 'Medium Risk',
    2: 'High Risk'
}
data['Risk Category'] = data['Cluster'].map(cluster_map)

# Step 7: Save clustered data to MongoDB
clustered_collection_name = "clustered_stock_data"  # New collection name for clustered data
clustered_collection = db[clustered_collection_name]

# Convert the data to a dictionary and insert into MongoDB
clustered_records = data.to_dict("records")
clustered_collection.insert_many(clustered_records)

print(f"Clustered data successfully saved to MongoDB collection: {clustered_collection_name}")
 