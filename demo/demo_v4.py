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
        '''
        Robo Advisor is your smart assistant for building an investment portfolio tailored to your needs.
        Whether you're aiming for growth, stability, or regular income, we’ve got you covered.
        Navigate through the tabs to learn more and create your personalized portfolio!
        '''
    )

    # Section: What is Investing?
    st.header("What is Investing?")
    st.write(
        '''
        Investing involves allocating resources, usually money, with the expectation of generating income or profit.
        It includes purchasing assets like stocks, bonds, real estate, or commodities that may appreciate over time.
        '''
    )
    st.image(
        "https://source.unsplash.com/800x400/?investment,finance",
        caption="Investing builds financial security. (Image source: Unsplash)",
        use_column_width=True,
    )

    # Section: Types of Investments
    st.header("Types of Investments")
    st.write(
        '''
        There are various investment vehicles available to investors:
        '''
    )
    st.markdown(
        '''
        - **Stocks**: Shares representing ownership in a company.
        - **Bonds**: Debt securities issued by entities to raise capital.
        - **Real Estate**: Property investments generating rental income or capital appreciation.
        - **Mutual Funds**: Pooled funds managed by professionals investing in diversified assets.
        - **Exchange-Traded Funds (ETFs)**: Funds traded on stock exchanges holding a basket of assets.
        - **Commodities**: Physical goods like gold, oil, or agricultural products.
        '''
    )

    # Section: What is an ETF?
    st.header("What is an ETF?")
    st.write(
        '''
        An Exchange-Traded Fund (ETF) is a type of investment fund that holds a collection of assets, such as stocks or bonds,
        and is traded on stock exchanges. ETFs offer diversification and are available in various types in Canada and the U.S.,
        including index ETFs, sector ETFs, and commodity ETFs.
        '''
    )
    st.image(
        "https://source.unsplash.com/800x400/?etf,investment",
        caption="ETFs provide diversified investment opportunities. (Image source: Unsplash)",
        use_column_width=True,
    )

    # Section: What are Commodities?
    st.header("What are Commodities?")
    st.write(
        '''
        Commodities are basic goods used in commerce that are interchangeable with other goods of the same type.
        They include natural resources like oil, natural gas, gold, and agricultural products.
        Investing in commodities can be done directly through physical purchase, futures contracts, or commodity-focused ETFs.
        '''
    )
    st.image(
        "https://source.unsplash.com/800x400/?commodities,trade",
        caption="Commodities are essential goods traded globally. (Image source: Unsplash)",
        use_column_width=True,
    )

    # Section: Trusted Trading Platforms
    st.header("Trusted Trading Platforms")
    st.write(
        '''
        Selecting a reliable trading platform is crucial for effective investing. Some of the most trusted platforms include:
        '''
    )
    st.markdown(
        '''
        - **Interactive Brokers**: Offers a wide range of investment options with competitive fees.
        - **Fidelity**: Known for its robust research tools and customer service.
        - **E*TRADE**: Provides user-friendly platforms suitable for both beginners and advanced traders.
        - **TD Ameritrade**: Offers comprehensive educational resources and trading tools.
        '''
    )
    st.image(
        "https://source.unsplash.com/800x400/?trading,platform",
        caption="Choosing the right trading platform is key to successful investing. (Image source: Unsplash)",
        use_column_width=True,
    )

    # Section: Why Use a Robo Advisor?
    st.header("Why Use a Robo Advisor?")
    st.write(
        '''
        A Robo Advisor simplifies investing by:
        - Providing personalized investment recommendations.
        - Reducing the complexity of asset allocation.
        - Optimizing your portfolio based on your goals and risk tolerance.
        '''
    )
    st.image(
        "https://source.unsplash.com/800x400/?robo,advisor",
        caption="Streamline your investments with Robo Advisor. (Image source: Unsplash)",
        use_column_width=True,
    )

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
