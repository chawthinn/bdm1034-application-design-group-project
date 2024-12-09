# clustering.py

import pandas as pd
from pymongo import MongoClient
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connect to MongoDB
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI not found in environment variables. Please check your .env file.")
DATABASE_NAME = "robo_advisor"
HISTORICAL_PRICES_COLLECTION = "historical_prices"
ASSET_METADATA_COLLECTION = "asset_metadata"

def fetch_unique_tickers():
    """
    Fetch distinct tickers from the 'historical_prices' collection.
    """
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    historical_prices_collection = db[HISTORICAL_PRICES_COLLECTION]

    tickers = historical_prices_collection.distinct('Asset')
    return tickers

def fetch_combined_data(assets):
    """
    Fetch asset metadata and historical prices, and combine them into one DataFrame.
    """
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]

    # Fetch historical prices (market data) for the given assets
    asset_metadata_collection = db[ASSET_METADATA_COLLECTION]
    asset_data = []

    for asset in assets:
        metadata = asset_metadata_collection.find_one({"Ticker": asset})
        if metadata:
            data = {
                'Ticker': metadata['Ticker'],
                'Monthly_Volatility': metadata.get('Monthly_Volatility', None),
                'Beta': metadata.get('Beta', None),
                'Sharpe_Ratio': metadata.get('Sharpe_Ratio', None),
                'Risk_Level': metadata.get('Risk_Level', None)
            }
            asset_data.append(data)

    # Fetch historical prices (market data) for the given assets
    historical_prices_collection = db['historical_prices']
    historical_data = []

    for asset in assets:
        data = list(historical_prices_collection.find({"Asset": asset}))
        
        if len(data) == 0:
            print(f"No data found for asset: {asset}")
            continue  # Skip assets that don't have data
        
        df = pd.DataFrame(data)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df['Asset'] = asset

        # Reset index to avoid conflicts when concatenating
        df.reset_index(inplace=True)  # Make 'Date' a column, not the index

        # Rename 'Asset' column to make it unique
        df.rename(columns={'Asset': f'{asset}_Asset'}, inplace=True)

        historical_data.append(df)

    if len(historical_data) == 0:
        print("No historical data found for any assets.")
        return pd.DataFrame()  # Return an empty DataFrame if no historical data exists

    # Combine both dataframes (fundamental data + market data)
    asset_metadata_df = pd.DataFrame(asset_data)

    # Concatenate all the historical data along columns, ensuring 'Date' is aligned
    historical_prices_df = pd.concat(historical_data, axis=1, join='inner')  # 'inner' to align on common dates

    # Merge asset metadata (fundamental data) with historical price data (market data)
    combined_df = pd.merge(historical_prices_df, asset_metadata_df, left_on=f'{assets[0]}_Asset', right_on='Ticker', how='left')

    return combined_df



def preprocess_and_cluster(combined_df):
    """
    Preprocess the combined data and perform clustering (K-Means and GMM).
    """
    # Extract relevant features for clustering
    # Select all numeric columns (both fundamental and market features)
    feature_columns = combined_df.select_dtypes(include=[float, int]).columns

    # Extract features for clustering
    features_df = combined_df[feature_columns]
    
    # Fill missing values if any (you can choose more advanced imputation techniques)
    features_df = features_df.fillna(features_df.mean())

    # Standardize the features (important for clustering algorithms like K-Means)
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features_df)

    # Apply PCA for dimensionality reduction (optional but useful for visualization)
    pca = PCA(n_components=2)
    features_pca = pca.fit_transform(features_scaled)

    # Perform K-Means Clustering
    kmeans = KMeans(n_clusters=3, random_state=42)
    clusters_kmeans = kmeans.fit_predict(features_scaled)

    # Perform Gaussian Mixture Model (GMM) Clustering
    gmm = GaussianMixture(n_components=3, random_state=42)
    clusters_gmm = gmm.fit_predict(features_scaled)

    # Visualize the results
    plt.figure(figsize=(10, 6))

    # K-Means Clustering Visualization
    plt.subplot(1, 2, 1)
    plt.scatter(features_pca[:, 0], features_pca[:, 1], c=clusters_kmeans, cmap='viridis')
    plt.title('K-Means Clustering')

    # GMM Clustering Visualization
    plt.subplot(1, 2, 2)
    plt.scatter(features_pca[:, 0], features_pca[:, 1], c=clusters_gmm, cmap='plasma')
    plt.title('Gaussian Mixture Clustering')

    plt.show()

# Main Code Execution

# Step 1: Fetch unique tickers from historical_prices collection
assets = fetch_unique_tickers()  # Get the list of unique tickers
print(f"Fetched tickers: {assets}")

# Step 2: Fetch and combine the asset metadata and historical price data for these tickers
combined_df = fetch_combined_data(assets)

# Step 3: Preprocess the combined data and perform clustering (K-Means and GMM)
preprocess_and_cluster(combined_df)
