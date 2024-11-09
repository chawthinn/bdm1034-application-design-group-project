import yfinance as yf
import pandas as pd

def gather_historical_data_yfinance(assets, start_date="2000-01-01", end_date="2024-11-07"):
    
    all_data = []

    for asset in assets:
        # Fetching data from Yahoo Finance
        df = yf.download(asset, start=start_date, end=end_date)
        
        if not df.empty:
            # Store only the 'Close' column and add asset
            df = df[['Close']]
            df['Asset'] = asset
            df = df.reset_index()  # Make Date a column
            
            all_data.append(df)
            print(f"Historical data for {asset} gathered successfully.")
        else:
            print(f"No data found for {asset} on Yahoo Finance.")

    # Combine all data into one DataFrame
    if all_data:
        historical_data = pd.concat(all_data, ignore_index=True)
        historical_data.to_csv("historical_price_data.csv", index=False)
        print("All historical data saved to historical_price_data.csv.")
        return historical_data
    else:
        print("No data was gathered for any asset.")
        return pd.DataFrame()

# Expanded list of assets
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

# Data gathering with Yahoo Finance
historical_data = gather_historical_data_yfinance(assets)
