from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pymongo import MongoClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
input_collection_name = "asset_data"
output_collection_name = "processed_asset_data"

# MongoDB Client Setup for Output
client = MongoClient(mongo_uri)
db = client[database_name]
output_collection = db[output_collection_name]

# Set up Spark Session
spark = SparkSession.builder \
    .appName("PySparkMongoProcessing") \
    .config("spark.mongodb.input.uri", f"{mongo_uri}.{input_collection_name}") \
    .config("spark.mongodb.output.uri", f"{mongo_uri}.{output_collection_name}") \
    .getOrCreate()

# Load Data from MongoDB
logging.info("Fetching data from MongoDB...")
data = spark.read.format("mongo").load()

# Drop MongoDB-specific ID Column
logging.info("Dropping '_id' column...")
data = data.drop("_id")

# Handle Missing Values
logging.info("Dropping rows with missing values...")
data = data.na.drop()

# Ensure Datetime Format
logging.info("Converting 'Date' column to datetime format...")
data = data.withColumn("Date", to_date(col("Date")))

# Sort Data by 'Date' and 'Ticker'
logging.info("Sorting data by 'Date' for each Ticker...")
data = data.orderBy(["Ticker", "Date"])

# Keep Relevant Columns
logging.info("Filtering relevant columns...")
columns_to_keep = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker", "Category"]
data = data.select(*columns_to_keep)

# Convert to Pandas for Saving to MongoDB
logging.info("Converting processed data to Pandas DataFrame...")
processed_df = data.toPandas()

# Save Processed Data to MongoDB
logging.info("Saving processed data back to MongoDB...")
processed_records = processed_df.to_dict("records")
output_collection.insert_many(processed_records)

logging.info("Data preprocessing complete!")

# Stop Spark Session
spark.stop()
