from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable 
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.operations import UpdateOne

# MongoDB connection details
MONGO_URI = Variable.get("MONGO_URI")
DB_NAME = Variable.get("DB_NAME")
COLLECTION_NAME = Variable.get("COLLECTION_NAME")

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
    description='Download historical stock data using Yahoo Finance and store in MongoDB',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=['stocks', 'finance', 'mongodb']
)
def stock_data_pipeline():
    
    @task
    def get_asset_list():
        assets = [
            # Technology
            "AAPL", "MSFT", "GOOGL", "IBM", "TSLA",
            
            # Healthcare
            "JNJ", "PFE", "MRK",
            
            # Financial
            "JPM", "BAC", "C",
            
            # Consumer Goods
            "PG", "KO", "PEP",
            
            # Broad Market ETFs
            "SPY", "QQQ", "IWM", "DIA",
            
            # Sector-Specific ETFs
            "XLF", "XLK", "XLE", "XLV", "XLY",
            
            # International and Emerging Market ETFs
            "VEA", "VWO", "EFA", "EEM",
            
            # Treasury Bond ETFs
            "TLT", "IEF", "SHY", "BND",
            
            # Corporate Bond ETFs
            "LQD", "HYG", "AGG"
        ]
        return assets

    @task
    def download_stock_data(asset: str) -> dict:
        """Download historical data for a single asset"""
        try:
            df = yf.download(
                asset,
                start="2000-01-01",
                end=datetime.now().strftime("%Y-%m-%d"),
                progress=False
            )
            
            if not df.empty:
                # Store only the 'Close' column and add asset
                df = df[['Close']]
                df['Asset'] = asset
                df = df.reset_index()  # Make Date a column
                
                # Convert DataFrame to list of dictionaries
                records = df.to_dict(orient='records')
                
                # Convert datetime objects to strings for MongoDB
                for record in records:
                    record['Date'] = record['Date'].strftime('%Y-%m-%d')
                
                result = {
                    'asset': asset,
                    'data': records,
                    'status': 'success'
                }
                logging.info(f"Successfully downloaded data for {asset}")
            else:
                result = {
                    'asset': asset,
                    'data': [],
                    'status': 'empty'
                }
                logging.warning(f"No data found for {asset}")
            
            return result
            
        except Exception as e:
            logging.error(f"Error downloading data for {asset}: {str(e)}")
            return {
                'asset': asset,
                'data': [],
                'status': 'error',
                'error': str(e)
            }

    @task
    def save_to_mongodb(results: list) -> None:
        """Save all downloaded data to MongoDB"""
        try:
            # Connect to MongoDB
            client = MongoClient(MONGO_URI)
            db = client[DB_NAME]
            collection = db[COLLECTION_NAME]
            
            # Get current date for the update
            update_date = datetime.now().strftime('%Y-%m-%d')
            
            successful_updates = 0
            failed_updates = 0
            
            # Correctly iterate over the list of results
            for result_dict in results:
                if result_dict['status'] == 'success':
                    asset = result_dict['asset']
                    records = result_dict['data']
                    
                    try:
                        # Create a bulk write operation
                        bulk_operations = []
                        for record in records:
                            # Create a filter to find existing record
                            filter_doc = {
                                'Asset': asset,
                                'Date': record['Date']
                            }
                            
                            # Update document with new data and metadata
                            update_doc = {
                                '$set': {
                                    **record,
                                    'last_updated': update_date,
                                    'last_updated_timestamp': datetime.now()
                                }
                            }
                            
                            # Add upsert operation to bulk operations list
                            bulk_operations.append(
                                UpdateOne(filter_doc, update_doc, upsert=True)
                            )
                        
                        # Execute bulk write operation
                        if bulk_operations:
                            result = collection.bulk_write(bulk_operations)
                            successful_updates += result.modified_count + result.upserted_count
                            logging.info(f"Successfully updated {asset} data in MongoDB")
                    
                    except PyMongoError as e:
                        failed_updates += 1
                        logging.error(f"Error updating {asset} data in MongoDB: {str(e)}")
            
            # Log summary
            logging.info(f"MongoDB Update Summary - Successful: {successful_updates}, Failed: {failed_updates}")
            
        except PyMongoError as e:
            logging.error(f"Error connecting to MongoDB: {str(e)}")
            raise
        
        finally:
            # Close MongoDB connection
            if 'client' in locals():
                client.close()

    # Define the workflow
    assets = get_asset_list()
    stock_data = download_stock_data.expand(asset=assets)
    save_to_mongodb(stock_data)

# Create the DAG
stock_data_dag = stock_data_pipeline()