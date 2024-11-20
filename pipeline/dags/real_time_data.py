from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import pytz
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError

def is_market_open():
    """Check if the US stock market is currently open."""
    now = datetime.now(pytz.timezone("US/Eastern"))
    # Market open and close times in Eastern Time
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    # Check if today is a weekday (Monday to Friday) and within market hours
    return now.weekday() < 5 and market_open <= now < market_close

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    description='Collect real-time stock data and save to MongoDB',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['stocks', 'yfinance', 'mongodb', 'real-time']
)
def real_time_stock_data():
    
    @task
    def get_asset_list():
        """Return the list of assets to collect data for."""
        assets = [
            # Technology
            "AAPL", "MSFT", "GOOGL", "IBM", "TSLA",
            # Healthcare
            "JNJ", "PFE", "MRK", "UNH",
            # Financial
            "JPM", "BAC", "C", "GS", "MS",
            # Consumer Goods
            "PG", "KO", "PEP", "WMT", "COST",
            # Broad Market ETFs
            "SPY", "QQQ", "IWM", "DIA",
            # Sector-Specific ETFs
            "XLF", "XLK", "XLE", "XLV", "XLY",
            # International and Emerging Market ETFs
            "VEA", "VWO", "EFA", "EEM",
            # Treasury Bond ETFs
            "TLT", "IEF", "SHY", "BND",
            # Corporate Bond ETFs
            "LQD", "HYG", "AGG",
            # Commodities
            "GLD", "SLV", "USO", "UNG", "PDBC"
        ]
        return assets

    @task
    def collect_and_save_data(assets):
        """Collect real-time data and save it to MongoDB."""
        if is_market_open():
            current_time = datetime.now(pytz.timezone("US/Eastern"))
            current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            price_data = {'Time': current_time_str}
            logging.info(f"Collecting real-time data at {current_time_str}")
            
            # Fetch current prices for each asset
            for asset in assets:
                try:
                    ticker = yf.Ticker(asset)
                    todays_data = ticker.history(period="1d")
                    if not todays_data.empty:
                        # Get the latest close price
                        price = todays_data['Close'].iloc[-1]
                        price_data[asset] = price
                        logging.info(f"Real-time data for {asset} at {current_time_str}: {price}")
                    else:
                        logging.warning(f"No data found for {asset}")
                        price_data[asset] = None
                except Exception as e:
                    logging.error(f"Error fetching real-time data for {asset}: {e}")
                    price_data[asset] = None  # Log as None if fetch fails

            # Save data to MongoDB
            try:
                # Get MongoDB connection details from Airflow Variables
                MONGO_URI = Variable.get("MONGO_URI")
                DB_NAME = Variable.get("DB_NAME")
                COLLECTION_NAME = "real-time"  # As per your requirement
                
                # Connect to MongoDB
                client = MongoClient(MONGO_URI)
                db = client[DB_NAME]
                collection = db[COLLECTION_NAME]

                # Insert data
                collection.insert_one(price_data)
                logging.info(f"Real-time data inserted into MongoDB collection '{COLLECTION_NAME}'")
                
            except PyMongoError as e:
                logging.error(f"Error saving data to MongoDB: {e}")
            finally:
                if 'client' in locals():
                    client.close()
        else:
            logging.info("Market is closed. No data collected.")

    # Define the workflow
    assets = get_asset_list()
    collect_and_save_data(assets)

# Instantiate the DAG
real_time_stock_data_dag = real_time_stock_data()
