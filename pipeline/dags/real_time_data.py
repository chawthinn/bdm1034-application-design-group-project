
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import yfinance as yf
from datetime import datetime, timedelta
import pytz
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.operations import UpdateOne

def is_market_open():
    """Check if the US stock market is currently open."""
    now = datetime.now(pytz.timezone("US/Eastern"))
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return now.weekday() < 5 and market_open <= now < market_close

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    description='Collect real-time stock data and save to MongoDB',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['stocks', 'yfinance', 'mongodb', 'real-time']
)
def real_time_stock_data():

    @task
    def get_asset_list():
        """Return the list of assets to collect data for."""
        return [
            "AAPL", "MSFT", "GOOGL", "IBM", "TSLA",
            "JNJ", "PFE", "MRK", "UNH",
            "JPM", "BAC", "C", "GS", "MS",
            "PG", "KO", "PEP", "WMT", "COST",
            "SPY", "QQQ", "IWM", "DIA",
            "XLF", "XLK", "XLE", "XLV", "XLY",
            "VEA", "VWO", "EFA", "EEM",
            "TLT", "IEF", "SHY", "BND",
            "LQD", "HYG", "AGG",
            "GLD", "SLV", "USO", "UNG", "PDBC"
        ]

    @task
    def collect_and_save_data(assets):
        """Collect real-time data and save it to MongoDB."""
        if is_market_open():
            logging.info("Market is open. Collecting data.")
            try:
                # MongoDB connection details from Airflow Variables
                MONGO_URI = Variable.get("MONGO_URI")
                DB_NAME = Variable.get("DB_NAME")
                COLLECTION_NAME = Variable.get("COLLECTION_NAME")
                
                client = MongoClient(MONGO_URI)
                db = client[DB_NAME]
                collection = db[COLLECTION_NAME]
                
                update_date = datetime.now().strftime('%Y-%m-%d')
                bulk_operations = []
                
                for asset in assets:
                    ticker = yf.Ticker(asset)
                    todays_data = ticker.history(period="1d")
                    if not todays_data.empty:
                        close_price = todays_data['Close'].iloc[-1]
                        current_date = todays_data.index[-1].strftime('%Y-%m-%d')
                        
                        record = {
                            "Asset": asset,
                            "Date": current_date,
                            "Close": close_price,
                            "last_updated": update_date,
                            "last_updated_timestamp": datetime.now()
                        }
                        
                        filter_doc = {"Asset": asset, "Date": current_date}
                        update_doc = {"$set": record}
                        bulk_operations.append(UpdateOne(filter_doc, update_doc, upsert=True))
                
                if bulk_operations:
                    collection.bulk_write(bulk_operations)
                    logging.info("Successfully saved real-time data to MongoDB.")
            except PyMongoError as e:
                logging.error(f"MongoDB error: {e}")
            except Exception as e:
                logging.error(f"Error collecting data: {e}")
            finally:
                if 'client' in locals():
                    client.close()
        else:
            logging.info("Market is closed. Skipping data collection.")

    # Define the workflow
    assets = get_asset_list()
    collect_and_save_data(assets)

# Instantiate the DAG
real_time_stock_data_dag = real_time_stock_data()
