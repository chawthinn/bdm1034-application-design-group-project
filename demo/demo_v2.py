import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt

# Set Streamlit Page Configuration - MUST be first
st.set_page_config(page_title="Robo Advisor", layout="wide")

# Load CSS for Professional Styling
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Load environment variables from .env file
load_dotenv()

# MongoDB Connection Details from .env
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = "robo_advisor"
COLLECTION_NAME = "clustered_stock_data"

@st.cache_resource
def fetch_data_from_mongodb():
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Fetch top 5 data from MongoDB
        cursor = collection.find().limit(5)  # Fetch the first 5 documents
        data = list(cursor)  # Convert cursor to a list
        return pd.DataFrame(data)  # Convert to pandas DataFrame
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

# Fetch Data
df = fetch_data_from_mongodb()

# Load Custom CSS
local_css("styles.css")  # Load CSS (create a `styles.css` file with your design)

# Set up Pages
if "page" not in st.session_state:
    st.session_state.page = "questionnaire"

if st.session_state.page == "questionnaire":
    st.title("Welcome to Robo Advisor")
    st.subheader("Tell us about your investment preferences")

    # Questionnaire Inputs
    investment_amount = st.number_input("How much do you want to invest? (USD)", min_value=1000, step=500)
    investment_goal = st.selectbox("What is your investment goal?", ["Growth", "Income", "Stability"])
    risk_tolerance = st.selectbox("What is your risk tolerance?", ["Low", "Medium", "High"])
    submit_button = st.button("Show My Suggested Portfolio")

    if submit_button:
        st.session_state.investment_amount = investment_amount
        st.session_state.investment_goal = investment_goal
        st.session_state.risk_tolerance = risk_tolerance
        st.session_state.page = "portfolio"

if st.session_state.page == "portfolio":
    st.title("Your Suggested Portfolio")
    st.subheader(f"Based on your inputs: Goal - {st.session_state.investment_goal}, Risk Tolerance - {st.session_state.risk_tolerance}")
    
    # Filter Data Based on Risk Tolerance
    if st.session_state.risk_tolerance == "Low":
        filtered_df = df[df["Risk Category"] == "Low Risk"]
    elif st.session_state.risk_tolerance == "Medium":
        filtered_df = df[df["Risk Category"] == "Medium Risk"]
    else:
        filtered_df = df[df["Risk Category"] == "High Risk"]

    # Display Top Stocks
    st.write("Here are the top recommended stocks:")
    if not filtered_df.empty:
        st.dataframe(filtered_df[["Ticker", "Category", "Momentum", "PE_Ratio", "Risk Category"]])

        # Pie Chart of Sector Allocation
        st.subheader("Portfolio Sector Allocation")
        sector_counts = filtered_df["Category"].value_counts()
        fig, ax = plt.subplots()
        ax.pie(sector_counts, labels=sector_counts.index, autopct="%1.1f%%", startangle=90)
        ax.axis("equal")
        st.pyplot(fig)

        # Visualize Performance Metrics
        st.subheader("Performance Metrics")
        st.bar_chart(filtered_df.set_index("Ticker")[["Momentum", "PE_Ratio"]])

        # Investment Allocation
        st.subheader("Investment Allocation")
        allocation = filtered_df[["Ticker", "Momentum"]].copy()
        allocation["Investment"] = (allocation["Momentum"] / allocation["Momentum"].sum()) * st.session_state.investment_amount
        st.dataframe(allocation)

    else:
        st.warning("No stocks match your selected risk category.")

    # Button to Return to Questionnaire
    if st.button("Go Back"):
        st.session_state.page = "questionnaire"
