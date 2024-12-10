import streamlit as st
from streamlit_option_menu import option_menu
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt

# Set Streamlit Page Configuration
st.set_page_config(page_title="Robo Advisor", layout="wide")

# Load environment variables from .env file
load_dotenv()

# MongoDB Connection Details from .env
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME", "robo_advisor")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "clustered_stock_data")

@st.cache_resource
def fetch_data_from_mongodb():
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Fetch all data from MongoDB
        cursor = collection.find()  # Fetch all documents
        data = list(cursor)  # Convert cursor to a list
        return pd.DataFrame(data)  # Convert to pandas DataFrame
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

# Fetch Data
df = fetch_data_from_mongodb()

# Sidebar Menu for Navigation
with st.sidebar:
    selected_tab = option_menu(
        menu_title="Robo Advisor",  # Menu Title
        options=["Home", "Questionnaire", "Suggested Portfolio"],  # Tabs
        icons=["house", "clipboard", "bar-chart"],  # Icons
        menu_icon="cast",  # Menu Icon
        default_index=0,  # Default Tab
        orientation="vertical",
    )

# Home Tab
if selected_tab == "Home":
    st.title("Welcome to Robo Advisor")
    st.subheader("An intelligent investment portfolio generator.")
    st.write(
        """
        Robo Advisor provides personalized investment recommendations based on your goals, 
        risk tolerance, and preferences. Navigate through the tabs to get started!
        """
    )
    st.image("https://via.placeholder.com/800x300", caption="Your Financial Partner", use_column_width=True)

# Questionnaire Tab
elif selected_tab == "Questionnaire":
    st.title("Investment Questionnaire")
    st.subheader("Tell us about your investment preferences")

    # Questionnaire Inputs
    investment_amount = st.number_input("How much do you want to invest? (USD)", min_value=1000, step=500)
    investment_goal = st.selectbox("What is your investment goal?", ["Growth", "Income", "Stability"])
    risk_tolerance = st.selectbox("What is your risk tolerance?", ["Low", "Medium", "High"])
    submit_button = st.button("Submit Preferences")

    if submit_button:
        st.session_state.investment_amount = investment_amount
        st.session_state.investment_goal = investment_goal
        st.session_state.risk_tolerance = risk_tolerance
        st.success("Your preferences have been saved! Navigate to the Suggested Portfolio tab.")

# Suggested Portfolio Tab
elif selected_tab == "Suggested Portfolio":
    st.title("Your Suggested Portfolio")
    if "investment_amount" not in st.session_state:
        st.warning("Please complete the questionnaire first!")
    else:
        st.subheader(
            f"Based on your inputs: Goal - {st.session_state.investment_goal}, Risk Tolerance - {st.session_state.risk_tolerance}"
        )

        # Filter Data Based on Risk Tolerance
        if st.session_state.risk_tolerance == "Low":
            filtered_df = df[df["Risk Category"] == "Low Risk"]
        elif st.session_state.risk_tolerance == "Medium":
            filtered_df = df[df["Risk Category"] == "Medium Risk"]
        else:
            filtered_df = df[df["Risk Category"] == "High Risk"]

        # Display Top Stocks
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
            allocation["Investment"] = (
                allocation["Momentum"] / allocation["Momentum"].sum()
            ) * st.session_state.investment_amount
            st.dataframe(allocation)

        else:
            st.warning("No stocks match your selected risk category.")
