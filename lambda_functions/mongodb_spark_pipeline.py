import os
from pyspark.sql import SparkSession

# Fetch credentials from environment variables (recommended for security)
username = os.getenv("MONGO_USERNAME", "")
password = os.getenv("MONGO_PASSWORD", "")
cluster_address = "cluster0.s5hw0.mongodb.net"
database = "robo_advisor"
collection = "historical_prices"

# Construct the MongoDB URI
mongodb_uri = f"mongodb+srv://{username}:{password}@{cluster_address}/{database}?retryWrites=true&w=majority&appName=Cluster0"

# Initialize SparkSession with MongoDB connection URI
spark = SparkSession.builder \
    .appName("TestMongoDBConnection") \
    .config("spark.mongodb.read.connection.uri", mongodb_uri) \
    .config("spark.mongodb.read.collection", collection) \
    .config("spark.mongodb.write.connection.uri", mongodb_uri) \
    .config("spark.mongodb.write.collection", collection) \
    .getOrCreate()

# Test reading data from MongoDB
try:
    # Read data from MongoDB collection
    df = spark.read.format("mongodb").load()
    print("Connection successful. Data preview:")
    df.show(20)
except Exception as e:
    print("Error reading data from MongoDB:")
    print(e)
finally:
    # Stop the SparkSession
    spark.stop()
