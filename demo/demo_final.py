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

# Streamlit App Configuration
st.set_page_config(
    page_title="Robo Advisor - Portfolio Recommendation System",
    page_icon=":moneybag:",
    layout="wide",
)

# Custom CSS for improved UI
st.markdown(
    """
    <style>
    .main { 
        background-color: #f9f9f9; 
        display: flex; 
        justify-content: center; 
    }
    .block-container { 
        max-width: 900px; 
        margin: auto; 
        padding: 2rem 2rem; 
        background-color: #ffffff; 
        border-radius: 10px; 
        box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1); 
    }
    .stTabs [data-baseweb="tab"] { font-size: 18px; font-weight: bold; }
    h2 { color: #2a9d8f; }
    .stButton > button { 
        background-color: #2a9d8f; 
        color: white; 
        font-weight: bold; 
    }
    .stButton > button:hover { background-color: #21867a; }
    </style>
    """,
    unsafe_allow_html=True,
)

# App Title
st.title(":moneybag: Robo Advisor - Portfolio Recommendation System")

# Tab navigation logic
tab1, tab2 = st.tabs(["📝 Questionnaire", "📊 Suggested Portfolio"])

# Tab 1: Questionnaire
with tab1:
    st.header("Questionnaire")
    st.write("Please provide the necessary details to tailor your portfolio recommendation.")
    with st.container():
        with st.form("questionnaire_form"):
            user_age = st.slider("Enter Your Age", min_value=18, max_value=100, value=30)
            investment_amount = st.number_input(
                "Investment Amount (in $)", min_value=1000, step=1000, value=100000
            )
            investment_duration = st.number_input(
                "Investment Duration (in Years)", min_value=1, max_value=50, value=20
            )
            submitted = st.form_submit_button("Submit")
            if submitted:
                # Store values in session state to persist them and trigger transition to next tab
                st.session_state.user_age = user_age
                st.session_state.investment_amount = investment_amount
                st.session_state.investment_duration = investment_duration
                st.session_state.active_tab = "📊 Suggested Portfolio"  # Switch to the "Suggested Portfolio" tab

# Tab 2: Suggested Portfolio
if "active_tab" in st.session_state and st.session_state.active_tab == "📊 Suggested Portfolio":
    with st.expander("📊 Suggested Portfolio"):
        st.header("Suggested Portfolio")

        # Retrieve user inputs from session state
        user_age = st.session_state.user_age
        investment_amount = st.session_state.investment_amount
        investment_duration = st.session_state.investment_duration

        # Assign Risk Category based on user age
        if user_age < 30:
            risk_category = "Mid Risk"
            fake_data = {
                "Ticker": ["AAPL", "GOOG", "AMZN", "TSLA", "MSFT", "NFLX", "SPY", "VTI", "BND", "GDX"],
                "Category": ["Stock", "Stock", "Stock", "Stock", "Stock", "Stock", "ETF", "ETF", "Bond", "Commodity"],
                "Risk_Category": ["Mid Risk"] * 10,
            }
        elif user_age > 50:
            risk_category = "Low Risk"
            fake_data = {
                "Ticker": ["BND", "VTI", "SPY", "AGG", "TLT", "LQD", "VGSH", "SHY", "HYG", "LQD"],
                "Category": ["Bond", "ETF", "ETF", "Bond", "Bond", "Bond", "Bond", "Bond", "Bond", "Bond"],
                "Risk_Category": ["Low Risk"] * 10,
            }
        else:
            risk_category = "High Risk"
            fake_data = {
                "Ticker": ["TSLA", "AMZN", "NVDA", "AAPL", "MSFT", "GOOG", "NFLX", "SPY", "ARKK", "QQQ"],
                "Category": ["Stock", "Stock", "Stock", "Stock", "Stock", "Stock", "Stock", "ETF", "ETF", "ETF"],
                "Risk_Category": ["High Risk"] * 10,
            }

        # Convert the fake data into a DataFrame
        recommended_portfolio = pd.DataFrame(fake_data)

        # Display the recommended portfolio
        st.subheader("Recommended Portfolio:")
        st.dataframe(recommended_portfolio)
