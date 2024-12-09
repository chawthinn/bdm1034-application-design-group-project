import yfinance as yf
import pandas as pd
import datetime
import time
import pytz

def is_market_open():
    
    now = datetime.datetime.now(pytz.timezone("US/Eastern"))
    # Market open and close times in Eastern Time
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    # Check if today is a weekday (Monday to Friday) and within market hours
    return now.weekday() < 5 and market_open <= now < market_close

def gather_realtime_data(assets, interval=900):
    
    while True:
        if is_market_open():
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            price_data = {'Time': current_time}
            
            # Fetch current prices for each asset
            for asset in assets:
                try:
                    ticker = yf.Ticker(asset)
                    todays_data = ticker.history(period="1d")
                    price_data[asset] = todays_data['Close'].iloc[-1]  # Get latest close price
                    print(f"Real-time data for {asset} at {current_time}: {price_data[asset]}")
                except Exception as e:
                    print(f"Error fetching real-time data for {asset}: {e}")
                    price_data[asset] = None  # Log as None if fetch fails

            # Append data to CSV file
            df = pd.DataFrame([price_data])
            # FIXE-ME: Save the data frame to MongoDB Atlas instead of CSV
            df.to_csv("realtime_price_data.csv", mode='a', header=not pd.io.common.file_exists("realtime_price_data.csv"), index=False)
            print(f"Real-time data collected and saved at {current_time}")
        else:
            print("Market is closed. Waiting for the market to open...")

        # Wait for the specified interval before checking again
        time.sleep(interval)

# list of assets with additional stocks, ETFs, bonds, and commodities
assets = [
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

# Start real-time data collection
gather_realtime_data(assets)
