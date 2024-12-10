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

# Sidebar Navigation with Navigation Logic
with st.sidebar:
    selected_tab = option_menu(
        menu_title="Robo Advisor",  # Menu Title
        options=["Home", "Questionnaire", "Suggested Portfolio"],  # Tabs
        icons=["house", "clipboard", "bar-chart"],  # Icons
        menu_icon="cast",  # Sidebar Icon
        default_index=0,  # Default Tab
        orientation="vertical",
        styles={
            "container": {"padding": "0!important", "background-color": "#f9f9f9"},
            "icon": {"color": "white", "font-size": "16px"},
            "nav-link": {
                "font-size": "14px",
                "text-align": "left",
                "margin": "0px",
                "padding": "10px",
                "color": "#00509E",
                "font-weight": "bold",
                "--hover-color": "#003366",
            },
            "nav-link-selected": {"background-color": "#00509E", "color": "white"},
        },
    )

# Initialize session state for page navigation
if "page" not in st.session_state:
    st.session_state.page = "Home"

# Navigation Logic Based on Session State
if selected_tab == "Home":
    st.session_state.page = "Home"
elif selected_tab == "Questionnaire":
    st.session_state.page = "Questionnaire"
elif selected_tab == "Suggested Portfolio":
    st.session_state.page = "Suggested Portfolio"

# Home Tab
# Page Content
if st.session_state.page == "Home":
    # Hero Section
    st.markdown(
        """
        <style>
        .hero-title {
            font-size: 3rem;
            font-weight: bold;
            text-align: center;
            margin-top: 20px;
            color: #003366;
        }
        .hero-description {
            font-size: 1.2rem;
            text-align: center;
            margin-bottom: 30px;
            color: #555555;
        }
        .cta-button {
            display: flex;
            justify-content: center;
            margin-top: 20px;
        }
        .cta-button button {
            background-color: #00509E;
            color: white;
            font-size: 1rem;
            padding: 10px 20px;
            border-radius: 8px;
            border: none;
            cursor: pointer;
        }
        .cta-button button:hover {
            background-color: #003366;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="hero-title">Welcome to Robo Advisor</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="hero-description">An intelligent investment portfolio generator. '
        'Simplify your investing and grow your wealth with confidence.</div>',
        unsafe_allow_html=True,
    )
    # Use Streamlit button with custom CSS styling
    if st.button("Get Started"):
        st.session_state.page = "Questionnaire"  # Navigate to the Questionnaire page

    # Educational Content Sections as Cards
    st.markdown(
        """
        <style>
        .card {
            background-color: #f9f9f9;
            border: 1px solid #e1e1e1;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
            box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1);
        }
        .card-title {
            font-size: 1.5rem;
            font-weight: bold;
            margin-bottom: 10px;
            color: #003366;
        }
        .card-content {
            font-size: 1rem;
            color: #555555;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Card: What is Investing?
    st.markdown(
        """
        <div class="card">
            <div class="card-title">What is Investing?</div>
            <div class="card-content">
                Investing is the process of allocating resources, usually money, with the expectation of generating income or profit. 
                It involves purchasing assets like stocks, bonds, or real estate that may appreciate over time.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Card: What is an ETF?
    st.markdown(
        """
        <div class="card">
            <div class="card-title">What is an ETF?</div>
            <div class="card-content">
                An Exchange-Traded Fund (ETF) is a type of investment fund traded on stock exchanges, 
                holding a collection of assets such as stocks or bonds. ETFs offer diversification and low fees, 
                making them popular among investors in Canada and the US.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Card: What are Commodities?
    st.markdown(
        """
        <div class="card">
            <div class="card-title">What are Commodities?</div>
            <div class="card-content">
                Commodities are physical goods like gold, oil, or agricultural products that are traded in markets worldwide. 
                Investing in commodities can provide portfolio diversification and act as a hedge against inflation.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Card: Trusted Trading Platforms
    st.markdown(
        """
        <div class="card">
            <div class="card-title">Trusted Trading Platforms</div>
            <div class="card-content">
                Some of the most trusted platforms include:
                <ul>
                    <li>Interactive Brokers</li>
                    <li>Fidelity</li>
                    <li>E*TRADE</li>
                    <li>TD Ameritrade</li>
                </ul>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Call-to-Action Footer
    if st.button("Start Building Your Portfolio"):
        st.session_state.page = "Questionnaire"  # Navigate to the Questionnaire page

# Questionnaire Tab
elif st.session_state.page == "Questionnaire":
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
elif st.session_state.page == "Suggested Portfolio":
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
