import yfinance as yf
import pandas as pd
import numpy as np
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.expected_returns import mean_historical_return
from sklearn.model_selection import ParameterGrid
import streamlit as st

# Step 1: Data Collection
def get_data(tickers, start_date, end_date):
    data = yf.download(tickers, start=start_date, end=end_date)['Adj Close']
    return data

# Step 2: Feature Engineering
def calculate_statistics(data):
    # Calculate expected returns and sample covariance matrix for optimization
    mu = mean_historical_return(data)
    S = CovarianceShrinkage(data).ledoit_wolf()
    return mu, S

# Step 3: Portfolio Optimization
def optimize_portfolio(mu, S, risk_tolerance):
    ef = EfficientFrontier(mu, S)
    if risk_tolerance == 'low':
        weights = ef.min_volatility()
    elif risk_tolerance == 'medium':
        weights = ef.max_sharpe()
    else:  # high risk
        weights = ef.max_quadratic_utility(risk_aversion=0.1)
    
    cleaned_weights = ef.clean_weights()
    performance = ef.portfolio_performance()
    return cleaned_weights, performance

# Step 4: Evaluation and Tuning
def evaluate_and_tune_portfolio(mu, S, risk_tolerance):
    # Define hyperparameters for grid search
    if risk_tolerance == 'high':
        param_grid = {'risk_aversion': [0.05, 0.1, 0.2, 0.5, 1.0]}
    elif risk_tolerance == 'low':
        param_grid = {'target_volatility': [0.10, 0.15, 0.20, 0.25, 0.30]}
    else:
        param_grid = {'sharpe_target': [None]}  # No parameter tuning for medium risk

    grid = ParameterGrid(param_grid)

    # Initialize the best portfolio values
    best_weights = None
    best_performance = (0, 0, 0)  # (return, volatility, sharpe)
    max_sharpe_ratio = -np.inf

    for params in grid:
        ef = EfficientFrontier(mu, S)
        
        try:
            if risk_tolerance == 'low':
                ef.min_volatility()
            elif risk_tolerance == 'medium':
                ef.max_sharpe()
            else:  # high risk
                ef.max_quadratic_utility(risk_aversion=params['risk_aversion'])
            
            # Evaluate performance
            weights = ef.clean_weights()
            performance = ef.portfolio_performance()
            sharpe_ratio = performance[2]
            
            # Update if better Sharpe Ratio is found
            if sharpe_ratio > max_sharpe_ratio:
                max_sharpe_ratio = sharpe_ratio
                best_weights = weights
                best_performance = performance
                
        except Exception as e:
            # Skip any configurations that raise errors (e.g., infeasible optimization)
            continue

    return best_weights, best_performance

# Step 5: Streamlit Interface
def main():
    st.title('Robo Advisor - Optimal Portfolio Recommendation')
    st.write('This app provides you with an optimal portfolio based on your risk tolerance.')

    # Basic user questions to assess financial profile
    st.subheader('Tell us about yourself')
    age = st.slider('What is your age?', 18, 100, 25)
    income = st.selectbox('What is your annual income range?', ['< $50,000', '$50,000 - $100,000', '$100,000 - $200,000', '> $200,000'])
    investment_goal = st.selectbox('What is your main investment goal?', ['Retirement', 'Wealth Accumulation', 'Saving for a Purchase', 'Education'])
    risk_tolerance = st.selectbox('How would you describe your risk tolerance?', ['low', 'medium', 'high'])

    # Define tickers for a diversified portfolio (stocks, bonds, ETFs, commodities)
    tickers = ['AAPL', 'MSFT', 'TLT', 'GLD', 'SPY', 'VTI', 'BND', 'VOO']
    start_date = pd.to_datetime('2022-01-01')
    end_date = pd.to_datetime('2023-01-01')

    if st.button('Get Portfolio Recommendation'):
        tickers_list = [ticker.strip() for ticker in tickers]
        data = get_data(tickers_list, start_date, end_date)
        mu, S = calculate_statistics(data)

        # Optimize and tune the portfolio
        weights, performance = evaluate_and_tune_portfolio(mu, S, risk_tolerance)

        # Display results
        st.subheader('Optimized Portfolio Weights')
        for asset, weight in weights.items():
            st.write(f"{asset}: {weight:.2%}")

        st.subheader('Portfolio Performance')
        st.write(f"Expected Annual Return: {performance[0]:.2%}")
        st.write(f"Annual Volatility (Risk): {performance[1]:.2%}")
        st.write(f"Sharpe Ratio: {performance[2]:.2f}")

if __name__ == "__main__":
    main()
