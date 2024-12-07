
from pymongo import MongoClient
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import seaborn as sns

# Connect to MongoDB Atlas
def connect_to_mongo():
    client = MongoClient("mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client.robo_advisor
    return db

# Load Data from MongoDB Collections
def load_data(db):
    historical_prices = pd.DataFrame(list(db.historical_prices.find()))
    real_time = pd.DataFrame(list(db.real_time.find()))
    return historical_prices, real_time

# EDA (Exploratory Data Analysis)
def eda(historical_prices, real_time):
    print("Historical Prices Info:")
    print(historical_prices.info())
    print(historical_prices.describe())

    print("\nReal-Time Data Info:")
    print(real_time.info())
    print(real_time.describe())

    # Plot historical prices distribution
    sns.histplot(historical_prices['Close'], kde=True)
    plt.title('Distribution of Historical Close Prices')
    plt.show()

    # Check for missing values
    print("\nMissing Values in Historical Prices:")
    print(historical_prices.isnull().sum())

    print("\nMissing Values in Real-Time Data:")
    print(real_time.isnull().sum())

# Feature Engineering
def feature_engineering(historical_prices, real_time):
    # Convert 'Date' and 'Time' to pandas datetime
    historical_prices['Date'] = pd.to_datetime(historical_prices['Date'])
    real_time['Time'] = pd.to_datetime(real_time['Time'])

    # Merge datasets
    merged_data = pd.merge(historical_prices, real_time, left_on='Date', right_on='Time', how='inner')

    # Calculate daily returns and volatility
    merged_data['Daily_Return'] = merged_data.groupby('Asset')['Close'].pct_change()
    merged_data['Volatility'] = merged_data.groupby('Asset')['Daily_Return'].rolling(window=5).std().reset_index(0, drop=True)

    # Drop unnecessary columns
    merged_data = merged_data.drop(columns=['_id', 'id', 'last_updated', 'last_updated_timestamp'])
    merged_data = merged_data.dropna()

    return merged_data

# Scaling Features
def scale_features(data, feature_columns):
    scaler = StandardScaler()
    data[feature_columns] = scaler.fit_transform(data[feature_columns])
    return data

# Clustering
def apply_clustering(data, feature_columns, n_clusters=5):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    data['Cluster'] = kmeans.fit_predict(data[feature_columns])
    return data, kmeans

# Evaluate Clustering
def evaluate_clustering(data, feature_columns, kmeans):
    # Calculate silhouette score
    silhouette_avg = silhouette_score(data[feature_columns], data['Cluster'])
    print(f"Silhouette Score: {silhouette_avg}")

    # Visualize clusters
    sns.scatterplot(
        data=data,
        x=feature_columns[0],
        y=feature_columns[1],
        hue='Cluster',
        palette='tab10'
    )
    plt.title('Clusters Visualization')
    plt.show()

    return silhouette_avg

# Main Function
def main():
    # Connect to MongoDB
    db = connect_to_mongo()

    # Load data
    historical_prices, real_time = load_data(db)

    # Perform EDA
    eda(historical_prices, real_time)

    # Feature Engineering
    merged_data = feature_engineering(historical_prices, real_time)

    # Features for clustering
    feature_columns = ['Close', 'Daily_Return', 'Volatility']
    merged_data = scale_features(merged_data, feature_columns)

    # Apply clustering
    merged_data, kmeans = apply_clustering(merged_data, feature_columns, n_clusters=5)

    # Evaluate clustering
    silhouette_avg = evaluate_clustering(merged_data, feature_columns, kmeans)

    # Save results
    output_path = 'final_portfolio_clusters.csv'
    merged_data.to_csv(output_path, index=False)
    print(f"Results saved to {output_path}")

if __name__ == "__main__":
    main()
