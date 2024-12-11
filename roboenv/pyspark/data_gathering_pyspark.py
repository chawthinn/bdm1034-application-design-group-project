from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import yfinance as yf
import logging
from pymongo import MongoClient

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
database_name = "robo_advisor"
collection_name = "asset_data"

# MongoDB Client Setup for Output
client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

# Set up Spark Session
spark = SparkSession.builder \
    .appName("PySparkAssetDataCollection") \
    .getOrCreate()

# Expanded list of diversified assets
assets = {
    "Technology": ["AAPL", "MSFT", "GOOGL", "IBM", "TSLA", "NVDA", "AMZN", "ADBE", "INTC", "META"],
    "Healthcare": ["JNJ", "PFE", "MRK", "UNH", "BMY", "AMGN", "ABBV", "GILD", "BIIB"],
    "Financial": ["JPM", "BAC", "C", "GS", "MS", "WFC", "AXP", "BK", "TFC", "BLK"],
    "Consumer Goods": ["PG", "KO", "PEP", "WMT", "COST", "MCD", "HD", "TGT", "NKE", "SBUX"],
    "Broad Market ETFs": ["SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "IVV", "SCHB", "VXF"],
    "Sector-Specific ETFs": ["XLF", "XLK", "XLE", "XLV", "XLY", "XLC", "XLRE", "XLB", "XLP", "XLU"],
    "International and Emerging Market ETFs": ["VEA", "VWO", "EFA", "EEM", "ACWI", "VXUS", "SPDW", "IEMG", "SCZ"],
    "Treasury Bond ETFs": ["TLT", "IEF", "SHY", "BND", "GOVT", "SCHR", "VGIT", "SPTL", "VGSH"],
    "Corporate Bond ETFs": ["LQD", "HYG", "AGG", "VCIT", "VCLT", "SJNK", "BKLN", "FLOT"],
}

# Function to fetch historical data for a given ticker
def fetch_data(ticker, start="2000-01-01", end="2023-12-31", interval="1d"):
    try:
        logging.info(f"Fetching raw data for {ticker}...")
        # Fetch non-adjusted data
        data = yf.Ticker(ticker).history(start=start, end=end, interval=interval, actions=False)
        
        if data.empty:
            logging.warning(f"No data found for {ticker}. Skipping...")
            return None
        
        # Include only relevant columns
        data = data[["Open", "High", "Low", "Close", "Volume"]].copy()
        data["Ticker"] = ticker  # Add ticker symbol
        data.reset_index(inplace=True)  # Reset index for Spark compatibility
        return data
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return None

# Fetch data for all assets and save to MongoDB
all_data = []

for category, tickers in assets.items():
    for ticker in tickers:
        data = fetch_data(ticker)
        if data is not None:
            data["Category"] = category  # Add category for each asset
            data.dropna(inplace=True)  # Drop missing values
            all_data.append(data)

# Combine all collected data into a single DataFrame
if all_data:
    logging.info("Combining all collected data...")
    combined_data = pd.concat(all_data)

    # Convert Pandas DataFrame to PySpark DataFrame
    spark_df = spark.createDataFrame(combined_data)

    # Write DataFrame to MongoDB
    logging.info("Saving processed data to MongoDB...")
    for row in spark_df.rdd.toLocalIterator():
        collection.insert_one(row.asDict())

logging.info("Data collection and saving to MongoDB complete.")
spark.stop()
