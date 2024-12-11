import streamlit as st
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import pickle
import logging
import plotly.express as px

# Load the trained K-Means model
model_filename = "kmeans_model.pkl"
with open(model_filename, "rb") as file:
    kmeans = pickle.load(file)

logging.info("Trained K-Means model loaded successfully.")

# Streamlit App Title
st.title("Robo Advisor - Portfolio Recommendation System")

# Tabs for the app
tab1, tab2, tab3 = st.tabs(["Questionnaire", "Suggested Portfolio", "Data Insights"])

# Tab 1: Questionnaire
with tab1:
    st.header("Questionnaire")

    # User Inputs
    user_age = st.number_input("Enter User Age", min_value=18, max_value=100, value=80)
    investment_amount = st.number_input("Enter Investment Amount", min_value=1000, step=1000, value=100000)
    investment_duration = st.number_input("Enter Investment Duration (Years)", min_value=1, max_value=50, value=20)

    st.write("User Input Summary:")
    st.write(f"Age: {user_age}, Investment Amount: {investment_amount}, Investment Duration: {investment_duration}")

# Load a dummy dataset (replace this with real MongoDB data if needed)
data = pd.DataFrame({
    "Beta": np.random.rand(100),
    "Alpha": np.random.rand(100),
    "Dividend_Yield": np.random.rand(100),
    "10-Day Volatility": np.random.rand(100),
})

# Add synthetic user features
data["User_Age"] = user_age
data["Investment_Amount"] = investment_amount
data["Investment_Duration"] = investment_duration

# Features to be used for clustering
required_features = ['Beta', 'Alpha', 'Dividend_Yield', '10-Day Volatility', 
                     'User_Age', 'Investment_Amount', 'Investment_Duration']

# Preprocess Data
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[required_features])

# Predict Clusters
data["Cluster"] = kmeans.predict(data_scaled)

# Tab 2: Suggested Portfolio
with tab2:
    st.header("Suggested Portfolio")

    # Example: Filter Portfolio Recommendations
    recommended_portfolio = data.groupby("Cluster").head(5)  # Top 5 recommendations per cluster
    st.subheader("Recommended Portfolio:")
    st.dataframe(recommended_portfolio[["Beta", "Alpha", "Cluster"]])

    # Visualization: Portfolio Distribution
    fig = px.scatter(
        data, x="Beta", y="Alpha", color="Cluster",
        title="Portfolio Distribution by Clusters",
        labels={"Beta": "Beta", "Alpha": "Alpha"},
        hover_data=["Dividend_Yield", "10-Day Volatility"]
    )
    st.plotly_chart(fig)

# Tab 3: Data Insights
with tab3:
    st.header("Data Insights")

    # Summary Stats
    st.subheader("Statistical Summary")
    st.write(data.describe())

    # Visualization: Risk Category Distribution
    fig = px.histogram(
        data, x="Cluster", title="Distribution of Clusters",
        labels={"Cluster": "Cluster Label"}
    )
    st.plotly_chart(fig)
