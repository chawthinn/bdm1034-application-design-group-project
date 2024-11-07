import requests
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Setup MongoDB connection
MONGO_URL = "mongodb+srv://user1:12345@cluster0.ensur.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URL)
db = client["robo_advisor_db"]
historical_data_col = db["historical_data"]
recommendations_col = db["recommendations"]
api_key = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=<api_key>'

# Data Collection function
def collect_stock_data(symbol, api_key):
    """
    Collects stock data from Alpha Vantage API.
    """
    print(f"Collecting data for {symbol}...")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key
    }
    response = requests.get(url, params=params)
    data = response.json().get("Time Series (Daily)", {})
    stock_data = pd.DataFrame.from_dict(data, orient="index")
    stock_data.columns = ["open", "high", "low", "close", "volume"]
    stock_data.index = pd.to_datetime(stock_data.index)
    stock_data = stock_data.apply(pd.to_numeric)
    return stock_data

# Data Processing function
def process_data(stock_data):
    """
    Processes data to calculate metrics like moving averages.
    """
    stock_data["moving_avg_30"] = stock_data["close"].rolling(window=30).mean()
    stock_data.dropna(inplace=True)
    print("Processed data with moving averages.")
    return stock_data

# Data Storage function
def store_data(stock_data):
    """
    Stores processed stock data in MongoDB.
    """
    print("Storing data in MongoDB...")
    records = stock_data.reset_index().to_dict("records")
    historical_data_col.insert_many(records)
    print("Data stored successfully.")

# Analysis and Recommendation
def analyze_and_recommend():
    """
    Analyzes data to make investment recommendations.
    """
    print("Generating recommendations...")
    stock_data = pd.DataFrame(list(historical_data_col.find()))
    stock_data.sort_values("index", inplace=True)
    
    # Sample strategy: Buy if the last close price is above the moving average
    last_close = stock_data["close"].iloc[-1]
    last_moving_avg = stock_data["moving_avg_30"].iloc[-1]
    recommendation = "Buy" if last_close > last_moving_avg else "Hold"
    
    # Save recommendation to MongoDB
    recommendations = {
        "symbol": STOCK_SYMBOL,
        "date": datetime.now(),
        "recommendation": recommendation,
        "last_close": last_close,
        "last_moving_avg": last_moving_avg
    }
    recommendations_col.insert_one(recommendations)
    print("Recommendation generated and stored.")

# Display Results
def display_latest_recommendation():
    """
    Fetches and displays the latest recommendation.
    """
    latest = recommendations_col.find().sort("date", -1).limit(1)
    for record in latest:
        print("\nLatest Recommendation:")
        print(f"Date: {record['date']}")
        print(f"Symbol: {record['symbol']}")
        print(f"Recommendation: {record['recommendation']}")
        print(f"Last Close: {record['last_close']}")
        print(f"30-Day Moving Average: {record['last_moving_avg']}")

# Run the Pipeline
if __name__ == "__main__":
    STOCK_SYMBOL = "AAPL"  # Stock symbol to track
    API_KEY = "YOUR_ALPHA_VANTAGE_API_KEY"  # Replace with your API key

    # Data Collection
    stock_data = collect_stock_data(STOCK_SYMBOL, API_KEY)
    
    # Data Processing
    processed_data = process_data(stock_data)
    
    # Data Storage
    store_data(processed_data)
    
    # Analysis and Recommendation
    analyze_and_recommend()
    
    # Display Results
    display_latest_recommendation()
