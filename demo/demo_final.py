import streamlit as st
import pandas as pd
import joblib

# Load models and scaler
@st.cache_resource
def load_resources():
    kmeans_model = joblib.load('kmeans_model.pkl')
    scaler = joblib.load('scaler.pkl')
    return kmeans_model, scaler

# Landing Page
def landing_page():
    st.title("Welcome to Robo Advisor")
    st.subheader("Your personalized portfolio optimization tool.")
    st.write(
        """
        Robo Advisor is designed to provide you with optimized investment portfolios tailored to your financial goals.
        
        ### Features:
        - **Data-driven portfolio clustering**: Using advanced machine learning.
        - **Real-time insights**: Updated portfolio suggestions based on the latest financial data.
        - **Progress tracking**: Monitor your investment journey.
        
        Explore the app using the tabs above to get started!
        """
    )

# Portfolio Optimization
def portfolio_page(kmeans_model, scaler):
    st.title("Portfolio Optimization")
    st.subheader("Provide your details to get a personalized portfolio.")
    
    # User Inputs
    age = st.number_input("Enter your age:", min_value=18, max_value=100, step=1)
    portfolio_size = st.number_input("Enter your portfolio size ($):", min_value=1000, step=100)
    time_horizon = st.number_input("Enter your time horizon (years):", min_value=1, step=1)

    if st.button("Generate Portfolio"):
        # Dummy data for demonstration
        sample_data = pd.DataFrame({
            "Beta": [0.5, 0.6, 0.4],
            "Alpha": [0.2, 0.3, 0.1],
            "Dividend_Yield": [0.05, 0.07, 0.04],
            "10-Day Volatility": [0.02, 0.03, 0.01]
        })

        # Scale and predict clusters
        scaled_data = scaler.transform(sample_data)
        sample_data['Cluster'] = kmeans_model.predict(scaled_data)

        # Display results
        st.subheader("Your Optimized Portfolio")
        st.write(sample_data)

# Progress Page
def progress_page():
    st.title("Your Investment Progress")
    st.subheader("Track your journey and measure your success.")
    st.write(
        """
        ### Progress Overview:
        - Total portfolio value
        - Performance by asset class
        - Risk-adjusted returns
        
        Visualizations and detailed metrics coming soon!
        """
    )
    st.info("Stay tuned for future updates to this feature!")

# Tabs for navigation
def main():
    kmeans_model, scaler = load_resources()

    tab1, tab2, tab3 = st.tabs(["🏠 Home", "📈 Portfolio Optimization", "📊 Progress"])
    
    with tab1:
        landing_page()
    
    with tab2:
        portfolio_page(kmeans_model, scaler)
    
    with tab3:
        progress_page()

if __name__ == "__main__":
    main()
