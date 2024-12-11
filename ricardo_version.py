import os
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from pymongo import MongoClient
import pickle
import logging
from dotenv import load_dotenv

# Configuração inicial
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Configuração MongoDB
mongo_uri = os.getenv("MONGO_URI")
database_name = "robo_advisor"
collection_name = "advanced_feature_engineered_data"

# Função para carregar dados
def load_data_from_mongo(mongo_uri, database_name, collection_name):
    try:
        client = MongoClient(mongo_uri)
        db = client[database_name]
        collection = db[collection_name]
        data = pd.DataFrame(list(collection.find()))
        logging.info("Data successfully loaded from MongoDB.")
        return data
    except Exception as e:
        logging.error(f"Error loading data from MongoDB: {e}")
        raise

# Função de processamento de dados
def preprocess_data(data):
    data.drop("_id", axis=1, inplace=True, errors="ignore")
    data["Daily Return"] = (data["Close"] - data["Open"]) / data["Open"]
    data["10-Day Volatility"] = data["Daily Return"].rolling(window=10).std()
    required_features = ["Beta", "Alpha", "Dividend_Yield", "10-Day Volatility"]
    for feature in required_features:
        data[feature] = data[feature].fillna(data[feature].mean())
    return data, required_features

# Função de clusterização
def apply_kmeans(data, features, n_clusters=3):
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(data[features])
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    kmeans.fit(scaled_data)
    data["Cluster"] = kmeans.labels_
    return data, kmeans, scaler

# Função para criar portfólio
def create_portfolio(data, risk_preference, top_n=8):
    filtered_data = data[data["Risk_Category"] == risk_preference]
    portfolio = filtered_data.nsmallest(top_n, "10-Day Volatility")
    return portfolio

# Main pipeline
if __name__ == "__main__":
    data = load_data_from_mongo(mongo_uri, database_name, collection_name)
    data, features = preprocess_data(data)
    
    # Adicionando dados do usuário
    user_params = {
        "User_Age": 35,
        "Investment_Amount": 50000,
        "Investment_Duration": 10
    }
    for key, value in user_params.items():
        data[key] = value
    
    # Clusterização
    data, kmeans, scaler = apply_kmeans(data, features)
    
    # Exportação do modelo
    with open("portfolio_model.pkl", "wb") as f:
        pickle.dump({"kmeans": kmeans, "scaler": scaler}, f)
    logging.info("Model successfully saved.")
