import streamlit as st
import numpy as np
import pandas as pd
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models, expected_returns
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices

asset_prices = pd.read_csv("historical_price_data.csv", parse_dates=True, index_col="Date")

st.title('Personalized Robo-Advisor: Portfolio Suggestion')

st.sidebar.header('Tell us about yourself:')
risk_tolerance = st.sidebar.selectbox('Risk Tolerance', ['Low', 'Medium', 'High'])
investment_horizon = st.sidebar.selectbox('Investment Horizon', ['Short-term', 'Medium-term', 'Long-term'])
investment_goal = st.sidebar.selectbox('Investment Goal', ['Growth', 'Income', 'Stability'])
initial_investment = st.sidebar.number_input('Initial Investment Amount (in USD)', min_value=1000, step=500)

st.header('Suggested Portfolio')

if risk_tolerance == 'Low':
    selected_assets = ['BND', 'AGG', 'TLT', 'LQD']  # Mostly bonds and stable ETFs
elif risk_tolerance == 'Medium':
    selected_assets = ['AAPL', 'MSFT', 'GOOGL', 'BND', 'TLT']  # Mix of stocks and bonds
else:  # High risk
    selected_assets = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN']  # Mostly tech stocks


prices = asset_prices[selected_assets]

mu = expected_returns.mean_historical_return(prices)
S = risk_models.sample_cov(prices)

ef = EfficientFrontier(mu, S)
weights = ef.max_sharpe()
cleaned_weights = ef.clean_weights()

st.subheader('Optimized Portfolio Weights:')
st.write(cleaned_weights)

expected_annual_return, annual_volatility, sharpe_ratio = ef.portfolio_performance()

st.subheader('Portfolio Performance Metrics:')
st.write(f"Expected Annual Return: {expected_annual_return * 100:.2f}%")
st.write(f"Annual Volatility (Risk): {annual_volatility * 100:.2f}%")
st.write(f"Sharpe Ratio: {sharpe_ratio:.2f}")

latest_prices = get_latest_prices(prices)
da = DiscreteAllocation(cleaned_weights, latest_prices, total_portfolio_value=initial_investment)
allocation, leftover = da.lp_portfolio()

st.subheader('Discrete Asset Allocation:')
st.write(f"Allocation: {allocation}")
st.write(f"Funds Remaining: ${leftover:.2f}")

st.header('Investment Recommendations')
if investment_goal == 'Growth':
    st.write("The suggested portfolio focuses on growth-oriented assets, aiming for capital appreciation over time. Expect higher volatility.")
elif investment_goal == 'Income':
    st.write("The portfolio includes income-generating assets, such as bonds and dividend-paying stocks, to provide regular income.")
else:
    st.write("This portfolio is designed for stability, minimizing risk while providing modest returns. Ideal for capital preservation.")

st.write("\n\n")
st.write("*Disclaimer: This is a simplified model and should not be considered financial advice. Please consult with a financial advisor for personalized investment strategies.")
