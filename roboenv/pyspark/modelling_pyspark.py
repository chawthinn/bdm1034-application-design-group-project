from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql.functions import col, when
from pyspark.ml.evaluation import ClusteringEvaluator
from pymongo import MongoClient
import matplotlib.pyplot as plt

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
collection_name = "feature_engineered_data"
clustered_collection_name = "clustered_stock_data"

# Set up Spark Session
spark = SparkSession.builder \
    .appName("KMeansClustering") \
    .config("spark.mongodb.input.uri", f"{mongo_uri}.{collection_name}") \
    .getOrCreate()

# Load Data from MongoDB
print("Loading data from MongoDB...")
data = spark.read.format("mongo").load()

# Drop MongoDB-specific ID column
data = data.drop("_id")

# Features to be used for clustering
features = ["Daily Return", "50-Day SMA", "10-Day Volatility", "50-Day Avg Volume", "RSI"]

# Step 1: Data Preprocessing
print("Preprocessing data...")
assembler = VectorAssembler(inputCols=features, outputCol="features_raw")
data = assembler.transform(data)

scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(data)
data = scaler_model.transform(data)

# Step 2: Determine the optimal number of clusters using the Elbow Method
print("Determining optimal number of clusters...")
inertia = []
K = range(2, 10)  # Testing for k from 2 to 9

for k in K:
    kmeans = KMeans(k=k, seed=42, featuresCol="features")
    model = kmeans.fit(data)
    inertia.append(model.summary.trainingCost)

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
optimal_k = 3  # Assuming 3 clusters based on the elbow plot
kmeans = KMeans(k=optimal_k, seed=42, featuresCol="features")
model = kmeans.fit(data)
data = model.transform(data)

# Step 4: Evaluate Clustering Performance
evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")
silhouette = evaluator.evaluate(data)
print(f"Silhouette Score: {silhouette:.2f}")

# Step 5: Map Clusters to Risk Categories
print("Mapping clusters to risk categories...")
cluster_map = {0: "Low Risk", 1: "Medium Risk", 2: "High Risk"}
mapping_expr = when(col("prediction") == 0, "Low Risk") \
    .when(col("prediction") == 1, "Medium Risk") \
    .otherwise("High Risk")
data = data.withColumn("Risk Category", mapping_expr)

# Step 6: Save clustered data to MongoDB
print("Saving clustered data to MongoDB...")
client = MongoClient(mongo_uri)
db = client[database_name]
clustered_collection = db[clustered_collection_name]

# Convert to Pandas for saving to MongoDB
data_pandas = data.select("Daily Return", "50-Day SMA", "10-Day Volatility", "50-Day Avg Volume", "RSI", "prediction", "Risk Category").toPandas()
data_pandas.rename(columns={"prediction": "Cluster"}, inplace=True)

# Insert data into MongoDB
clustered_records = data_pandas.to_dict("records")
clustered_collection.insert_many(clustered_records)

print(f"Clustered data successfully saved to MongoDB collection: {clustered_collection_name}")

# Stop Spark Session
spark.stop()
