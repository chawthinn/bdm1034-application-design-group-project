import streamlit as st
import pandas as pd
from sklearn.preprocessing import StandardScaler
import joblib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Load Resources
@st.cache_resource
def load_resources():
    """Load the K-Means model and Scaler."""
    kmeans_model = joblib.load("kmeans_model.pkl")
    scaler = joblib.load("scaler.pkl")
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

    if st.button("Generate Portfolio"):
        try:
            # Fetch real data (Replace with database call if needed)
            data = pd.read_csv("real_data.csv")  # Ensure `real_data.csv` exists and contains the necessary features

            # Add user-specific features
            data["User_Age"] = user_age
            data["Investment_Amount"] = investment_amount
            data["Investment_Duration"] = investment_duration

            # Preprocess data
            required_features = [
                "Beta", "Alpha", "Dividend_Yield", "10-Day Volatility", 
                "User_Age", "Investment_Amount", "Investment_Duration"
            ]
            scaled_data = scaler.transform(data[required_features])

            # Predict clusters
            data["Cluster"] = kmeans_model.predict(scaled_data)

            # Display cluster assignments
            st.subheader("Cluster Assignments")
            st.write(data[["Cluster"] + required_features].head())

            # Display cluster insights (Example logic, customize as needed)
            st.subheader("Cluster Insights")
            for cluster in data["Cluster"].unique():
                cluster_data = data[data["Cluster"] == cluster]
                st.write(f"Cluster {cluster} Summary:")
                st.write(cluster_data.describe())

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

    tab1, tab2, tab3 = st.tabs(["🏠 Home", "📈 Portfolio Optimization", "📊 Progress"])
    with tab1:
        landing_page()
    with tab2:
        portfolio_page(kmeans_model, scaler)
    with tab3:
        progress_page()

if __name__ == "__main__":
    main()
