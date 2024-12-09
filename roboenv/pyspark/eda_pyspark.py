from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
collection_name = "feature_engineered_data"

# Set up Spark Session
spark = SparkSession.builder \
    .appName("FeatureEngineeredDataAnalysis") \
    .config("spark.mongodb.input.uri", f"{mongo_uri}.{collection_name}") \
    .getOrCreate()

# Load Data from MongoDB into Spark DataFrame
print("Loading data from MongoDB...")
data = spark.read.format("mongo").load()

# Drop MongoDB-specific ID Column
print("Dropping '_id' column...")
data = data.drop("_id")

# Display Summary Statistics
print("Summary Statistics:")
data.describe().show()

# Check for Missing Values
print("\nChecking for Missing Values...")
missing_values = data.select([(col(c).isNull().cast("int")).alias(c) for c in data.columns]).groupBy().sum()
missing_values.show()

# Correlation Analysis for Feature-Engineered Columns
print("\nCalculating Correlations for Feature-Engineered Columns...")

# List of feature-engineered columns
feature_engineered_columns = [
    "Daily Return",
    "10-Day SMA",
    "50-Day SMA",
    "200-Day SMA",
    "10-Day Volatility",
    "10-Day Avg Volume",
    "50-Day Avg Volume",
    "RSI",
]

# Filter Data to Include Only Feature-Engineered Columns
feature_engineered_data = data.select(*feature_engineered_columns)

# Convert PySpark DataFrame to Pandas DataFrame for Visualization
print("\nConverting Spark DataFrame to Pandas for Correlation Analysis...")
pandas_data = feature_engineered_data.toPandas()

# Calculate Correlation Matrix
correlation_matrix = pandas_data.corr()

# Display Correlation Matrix
print("\nCorrelation Matrix:")
print(correlation_matrix)

# Visualize Correlation Matrix using Seaborn Heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(
    correlation_matrix,
    annot=True,
    fmt=".2f",
    cmap="coolwarm",
    cbar=True,
    square=True,
)
plt.title("Correlation Matrix for Feature-Engineered Columns")
plt.show()

# Stop Spark Session
spark.stop()
