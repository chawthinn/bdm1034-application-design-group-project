from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, mean, stddev, when
from pyspark.sql.window import Window
from pymongo import MongoClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
input_collection_name = "processed_asset_data"
output_collection_name = "feature_engineered_data"

# MongoDB Client Setup for Output
client = MongoClient(mongo_uri)
db = client[database_name]
output_collection = db[output_collection_name]

# Set up Spark Session
spark = SparkSession.builder \
    .appName("FeatureEngineering") \
    .config("spark.mongodb.input.uri", f"{mongo_uri}.{input_collection_name}") \
    .getOrCreate()

# Load Data from MongoDB into Spark DataFrame
logging.info("Fetching processed data from MongoDB...")
data = spark.read.format("mongo").load()

# Drop MongoDB-specific ID Column
data = data.drop("_id")

# Define Window Spec for Ticker and Date
window_spec = Window.partitionBy("Ticker").orderBy("Date")

# Calculate Daily Returns
logging.info("Calculating daily returns...")
data = data.withColumn("Daily Return", (col("Close") - col("Open")) / col("Open"))

# Calculate Rolling Averages and Standard Deviations
logging.info("Calculating rolling averages and standard deviations...")
data = data.withColumn("10-Day SMA", mean("Close").over(window_spec.rowsBetween(-9, 0)))
data = data.withColumn("50-Day SMA", mean("Close").over(window_spec.rowsBetween(-49, 0)))
data = data.withColumn("200-Day SMA", mean("Close").over(window_spec.rowsBetween(-199, 0)))
data = data.withColumn("10-Day Volatility", stddev("Daily Return").over(window_spec.rowsBetween(-9, 0)))

# Calculate Rolling Volume Averages
logging.info("Calculating rolling volume averages...")
data = data.withColumn("10-Day Avg Volume", mean("Volume").over(window_spec.rowsBetween(-9, 0)))
data = data.withColumn("50-Day Avg Volume", mean("Volume").over(window_spec.rowsBetween(-49, 0)))

# Calculate Relative Strength Index (RSI)
logging.info("Calculating RSI...")
delta = col("Close") - lag("Close", 1).over(window_spec)
gain = when(delta > 0, delta).otherwise(0)
loss = when(delta < 0, -delta).otherwise(0)
rolling_gain = mean(gain).over(window_spec.rowsBetween(-13, 0))
rolling_loss = mean(loss).over(window_spec.rowsBetween(-13, 0))
rs = rolling_gain / rolling_loss
data = data.withColumn("RSI", 100 - (100 / (1 + rs)))

# Drop Rows with NaN Values
logging.info("Dropping rows with missing values...")
data = data.na.drop()

# Convert DataFrame to Pandas for MongoDB Insertion
logging.info("Converting Spark DataFrame to Pandas...")
final_data = data.toPandas()

# Save Enhanced Data to MongoDB
logging.info("Saving feature-engineered data to MongoDB...")
output_collection.insert_many(final_data.to_dict("records"))

logging.info("Feature engineering complete!")
spark.stop()
