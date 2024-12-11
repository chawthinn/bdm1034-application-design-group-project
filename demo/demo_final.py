import streamlit as st
import pandas as pd
import joblib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Load Resources
@st.cache_resource
def load_resources():
    """Load the K-Means model and Scaler."""
    resources = joblib.load("portfolio_model.pkl")
    kmeans_model = resources["kmeans"]
    scaler = resources["scaler"]
    logging.info("Resources loaded successfully.")
    return kmeans_model, scaler

# Landing Page
def landing_page():
    st.title("Robo Advisor - User Portfolio Recommendation")
    st.subheader("Personalized investment insights at your fingertips.")
    st.write(
        """
        Welcome to Robo Advisor, where data-driven machine learning meets personalized investment strategies.
        
        ### Features:
        - **Cluster-based portfolio recommendations**
        - **Insightful analysis and tracking tools**
        - **Easy-to-use interface for investors of all levels**
        
        Explore the app using the tabs above.
        """
    )

# Portfolio Optimization
def portfolio_page(kmeans_model, scaler):
    st.title("Portfolio Optimization")
    st.subheader("Get your personalized portfolio recommendations.")

    # User Inputs
    user_age = st.number_input("Enter your age:", min_value=18, max_value=100, step=1)
    investment_amount = st.number_input("Enter your investment amount ($):", min_value=1000, step=1000)
    investment_duration = st.number_input("Enter your investment duration (years):", min_value=1, step=1)
    
    preferred_risk_category = st.selectbox(
        "Select your risk appetite:", ["Low Risk", "Medium Risk", "High Risk"]
    )

    if st.button("Generate Portfolio"):
        try:
            # Placeholder for real data (Replace with database call or predefined dataset)
            data = pd.DataFrame({
                "Ticker": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM"],
                "Beta": [1.2, 1.1, 0.9, 1.3, 1.5, 1.4, 1.2, 1.0],
                "Alpha": [0.05, 0.03, 0.04, 0.06, 0.07, 0.04, 0.05, 0.03],
                "Dividend_Yield": [0.01, 0.015, 0.012, 0.008, 0.005, 0.01, 0.007, 0.02],
                "10-Day Volatility": [0.02, 0.03, 0.025, 0.04, 0.05, 0.045, 0.035, 0.03]
            })

            # Debug: Display dataset for verification
            st.write("Dataset used for prediction:", data)

            # Preprocess data
            required_features = ["Beta", "Alpha", "Dividend_Yield", "10-Day Volatility"]
            scaled_data = scaler.transform(data[required_features])

            # Predict clusters
            data["Cluster"] = kmeans_model.predict(scaled_data)

            # Debug: Analyze cluster distribution
            cluster_counts = data["Cluster"].value_counts()
            st.write("Cluster distribution:", cluster_counts)

            # Map risk categories to clusters (Example mapping, verify based on model output)
            cluster_risk_mapping = {
                0: "Low Risk",
                1: "Medium Risk",
                2: "High Risk"
            }

            # Debug: Check mapping correctness
            st.write("Cluster to risk category mapping:", cluster_risk_mapping)

            data["Risk_Category"] = data["Cluster"].map(cluster_risk_mapping)

            # Add user-specific features for filtering
            data["User_Age"] = user_age
            data["Investment_Amount"] = investment_amount
            data["Investment_Duration"] = investment_duration

            # Filter by preferred risk category
            filtered_data = data[data["Risk_Category"] == preferred_risk_category]

            # Handle empty filtered data
            if filtered_data.empty:
                st.warning("No recommendations match your selected risk category.")
                return

            # Select top recommendations
            portfolio = filtered_data.nsmallest(8, "10-Day Volatility")

            # Display Portfolio
            st.subheader("Recommended Portfolio")
            st.write(portfolio[["Ticker", "Beta", "Alpha", "Dividend_Yield", "10-Day Volatility"]])

        except Exception as e:
            st.error(f"Error generating portfolio: {e}")

# Progress Tracking
def progress_page():
    st.title("Investment Progress")
    st.subheader("Track your journey and monitor performance.")
    st.info("This feature is under development. Stay tuned for updates!")

# Main App
def main():
    kmeans_model, scaler = load_resources()

    tab1, tab2, tab3 = st.tabs(["Home", "Portfolio Optimization", "Progress"])
    with tab1:
        landing_page()
    with tab2:
        portfolio_page(kmeans_model, scaler)
    with tab3:
        progress_page()

if __name__ == "__main__":
    main()
