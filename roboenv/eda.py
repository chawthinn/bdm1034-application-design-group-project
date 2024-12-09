import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient

# MongoDB Connection
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
collection_name = "feature_engineered_data"

# Load Data from MongoDB
client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

# Load Data into DataFrame
data = pd.DataFrame(list(collection.find()))

# Drop MongoDB-specific ID
if "_id" in data.columns:
    data.drop("_id", axis=1, inplace=True)

# Display Summary Statistics
print("Summary Statistics:")
print(data.describe())

# Check for Missing Values
missing_values = data.isnull().sum()
print("\nMissing Values:")
print(missing_values[missing_values > 0])

# import matplotlib.pyplot as plt

# # Define features for distribution plots
# features = ["Daily Return", "RSI", "10-Day Volatility"]

# # Plot histograms for each feature
# for feature in features:
#     plt.figure(figsize=(8, 5))
#     data[feature].hist(bins=50, alpha=0.7)
#     plt.title(f"Distribution of {feature}")
#     plt.xlabel(feature)
#     plt.ylabel("Frequency")
#     plt.grid(False)
#     plt.show()

# Correlation Analysis
print("\nCalculating Correlations for Feature-Engineered Columns...")

# Feature-engineered columns
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

# Filter data to include only feature-engineered columns
feature_engineered_data = data[feature_engineered_columns]

# Calculate the correlation matrix
correlation_matrix = feature_engineered_data.corr()

# Display the correlation matrix
print("\nCorrelation Matrix:")
print(correlation_matrix)

# Visualize the correlation matrix using a heatmap
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


