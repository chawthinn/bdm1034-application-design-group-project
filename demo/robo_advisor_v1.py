import streamlit as st
from pymongo import MongoClient
import pandas as pd
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models, expected_returns
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices

# MongoDB Connection Details
mongo_uri = "mongodb+srv://user1:12345@cluster0.s5hw0.mongodb.net/robo_advisor?retryWrites=true&w=majority"
database_name = "robo_advisor"
collection_name = "clustered_stock_data"

@st.cache_resource
def fetch_data_from_mongodb():
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[database_name]
        collection = db[collection_name]

        # Fetch top 5 data from MongoDB
        cursor = collection.find().limit(5)  # Fetch the first 5 documents
        data = list(cursor)  # Convert cursor to a list
        return pd.DataFrame(data)  # Convert to pandas DataFrame
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

# Fetch Data
df = fetch_data_from_mongodb()

# App Title
st.title('Personalized Robo-Advisor: Portfolio Suggestion')

# Show Top 5 Data
st.subheader('Top 5 Data from Clustered Stock Data')
if not df.empty:
    if "_id" in df.columns:
        df.drop(columns=["_id"], inplace=True)  # Drop the MongoDB-specific '_id' column
    st.dataframe(df)
else:
    st.error("No data available from MongoDB.")

# Sidebar User Inputs
st.sidebar.header('Tell us about yourself:')
risk_tolerance = st.sidebar.selectbox('Risk Tolerance', ['Low', 'Medium', 'High'])
investment_horizon = st.sidebar.selectbox('Investment Horizon', ['Short-term', 'Medium-term', 'Long-term'])
investment_goal = st.sidebar.selectbox('Investment Goal', ['Growth', 'Income', 'Stability'])
initial_investment = st.sidebar.number_input('Initial Investment Amount (in USD)', min_value=1000, step=500)

# Portfolio Suggestion Logic
st.header('Suggested Portfolio')

if risk_tolerance == 'Low':
    selected_assets = df[df['Risk Category'] == 'Low Risk']['Ticker'].tolist()
elif risk_tolerance == 'Medium':
    selected_assets = df[df['Risk Category'] == 'Medium Risk']['Ticker'].tolist()
else:  # High Risk
    selected_assets = df[df['Risk Category'] == 'High Risk']['Ticker'].tolist()

# Simulated Asset Prices (Replace with real data if available)
prices = pd.DataFrame()  # Replace with actual historical price data
if selected_assets:
    st.write(f"Selected Assets: {selected_assets}")
    # Placeholder logic: Simulate a price matrix for demonstration purposes
    dates = pd.date_range(start="2023-01-01", periods=30)
    prices = pd.DataFrame({asset: (100 + pd.Series(range(30))).values for asset in selected_assets}, index=dates)

if not prices.empty:
    # Optimize Portfolio
    mu = expected_returns.mean_historical_return(prices)
    S = risk_models.sample_cov(prices)

    ef = EfficientFrontier(mu, S)
    weights = ef.max_sharpe()
    cleaned_weights = ef.clean_weights()

    # Show Optimized Weights
    st.subheader('Optimized Portfolio Weights:')
    st.write(cleaned_weights)

    # Portfolio Performance Metrics
    expected_annual_return, annual_volatility, sharpe_ratio = ef.portfolio_performance()
    st.subheader('Portfolio Performance Metrics:')
    st.write(f"Expected Annual Return: {expected_annual_return * 100:.2f}%")
    st.write(f"Annual Volatility (Risk): {annual_volatility * 100:.2f}%")
    st.write(f"Sharpe Ratio: {sharpe_ratio:.2f}")

    # Discrete Asset Allocation
    latest_prices = get_latest_prices(prices)
    da = DiscreteAllocation(cleaned_weights, latest_prices, total_portfolio_value=initial_investment)
    allocation, leftover = da.lp_portfolio()

    st.subheader('Discrete Asset Allocation:')
    st.write(f"Allocation: {allocation}")
    st.write(f"Funds Remaining: ${leftover:.2f}")
else:
    st.warning("No historical price data available for the selected assets.")

# Investment Recommendations
st.header('Investment Recommendations')
if investment_goal == 'Growth':
    st.write("The suggested portfolio focuses on growth-oriented assets, aiming for capital appreciation over time. Expect higher volatility.")
elif investment_goal == 'Income':
    st.write("The portfolio includes income-generating assets, such as bonds and dividend-paying stocks, to provide regular income.")
else:
    st.write("This portfolio is designed for stability, minimizing risk while providing modest returns. Ideal for capital preservation.")

st.write("\n\n")
st.write("*Disclaimer: This is a simplified model and should not be considered financial advice. Please consult with a financial advisor for personalized investment strategies.")
