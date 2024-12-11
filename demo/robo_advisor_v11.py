import streamlit as st
from streamlit_option_menu import option_menu
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import requests

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

# Sidebar Navigation
with st.sidebar:
    selected_tab = option_menu(
        menu_title="Robo Advisor",  # Menu Title
        options=["Home", "Questionnaire", "Suggested Portfolio","About Us"],  # Tabs
        icons=["house", "clipboard", "bar-chart"],  # Icons
        menu_icon="cast",  # Menu Icon
        default_index=0,  # Default Tab
        orientation="vertical",
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
elif selected_tab == "About Us":
    st.session_state.page = "About Us"

# Home Tab with Submenu
if st.session_state.page == "Home":
    home_submenu = option_menu(
        menu_title="Home Menu",
        options=["Overview", "Definition of Terms", "How It Works", "FAQs"],
        icons=["info", "book", "gear", "question-circle"],
        menu_icon="house",
        default_index=0,
        orientation="horizontal",
    )

    if home_submenu == "Overview":
        st.title("Welcome to Robo Advisor")
        st.markdown(
            "Robo Advisor is your friendly, intelligent platform to help you navigate the world of investing. "
            "We simplify investing for beginners by providing tailored guidance and actionable insights."
        
        )
        #image from Google Drive
        file_id = "1FOqBjTfCAztbKTndvrMutY54z9yKg89n" 
        url = f"https://drive.google.com/uc?export=view&id={file_id}"

        try:
            response = requests.get(url)
            if response.status_code == 200:
                st.image(response.content, caption="Comparison of cash, savings, and investing over 30 years", use_container_width=True)
            else:
                st.warning("Failed to load the image. Please check the Google Drive link or file permissions.")
        except Exception as e:
            st.error(f"An error occurred while fetching the image: {e}")

    elif home_submenu == "Definition of Terms":
        st.title("Definition of Terms")
        st.markdown("""
        -  **Asset Allocation**: A portfolio’s mix of equities, fixed income, cash, and other asset classes. Your asset allocation should reflect your goals, risk profile, income needs, and other factors.
        -  **Balanced portfolio**: An investment portfolio that holds a mix of different types of investments, such as bonds and stocks.
        -  **Bonds**: Loans made to corporations or governments. They pay you interest over time and give your money back at the end.
        -  **Commodities**: Physical goods like gold or oil, traded in global markets. Investing in commodities can provide portfolio diversification and act as a hedge against inflation.
        -  **Cryptocurrency**: A cryptocurrency is a form of digital asset based on a network that is distributed across a large number of computers. This decentralized structure allows them to exist outside the control of governments and central authorities.
        -  **Dividend**: A portion of a company’s profit that a company decides to pay to shareholders.
        -  **Dividends**: Dividends are regular payments made by companies to their shareholders, usually from the company’s profits. If you own shares in a company that pays dividends, you receive a portion of its earnings, often quarterly.
        -  **ETFs**: Exchange-Traded Funds are funds that track indices and trade like stocks. ETFs offer diversification and low fees, making them popular among investors in Canada and the US.
        -  **Liquidity**: Refers to the availability of your money. The more liquid it is, the more available it is. Liquid assets or investments are those you are able to cash in or sell quickly, like savings accounts and most stocks. Liquidity can be important if you are planning to use your savings or investments in the short term.
        -  **Mutual Funds**: A pool of money from many investors, used to buy a mix of stocks, bonds, or other assets, managed by professionals.
        -  **Real Estate**: Investing in property like houses, apartments, or commercial buildings for rental income or future sale.
        -  **Stock exchange**: A public place where shares and some other types of investments can be bought and sold.
        -  **Stocks**: Shares of ownership in a company.
        """)
    elif home_submenu == "How It Works":
        st.title("How It Works")
        st.markdown("""
        1. **Answer a Few Questions**: Tell us about your investment preferences, goals, and risk tolerance.
        2. **Receive Suggestions**: Our platform generates personalized investment suggestions based on your input.
        3. **Learn and Explore**: Access educational content and make informed decisions.
        """)
    elif home_submenu == "FAQs":
        st.title("Frequently Asked Questions")
        st.markdown("""
        **Q: What is investing?**  
        **A:** Investing is putting money into financial assets with the aim of growing wealth over time.  
        
        **Q: What is the difference between Saving and Investing?**
        
        **A:**
           **Saving:** you put cash in the safest places, where you can access your money at any time. Your risk of losing money is low and you may earn a little interest.
           
           **Investing:** you put your money into an investment to make a financial gain. There’s more risk and no guarantee you will get your money back. But you may earn more money, too. This is called your ‘return.’

        **Q:** What does it mean to **"Diversify Your Investments"**?
       
        **A:** 
            **What it means:** Don't put all your money into one type of investment. Spread it across different types, like stocks, bonds, real estate, and cash.
            
        **A:**
            **Why:** Different investments react differently to market changes. If one investment loses value, others might go up, balancing out the risk.
        
        **Q:** **What are Short-term investments?**
        
        **A:** Short-term investments are assets that can be converted into cash or can be sold within a short period of time, typically within 1-3 years.

        **Q:** **What are Long-term investments?**
        
        **A:** Long term investments are assets that an individual or company intends to hold for a period of more than three years. Instruments facilitating long-term investments include stocks, real estate, cash, etc. Long-term investors take on a substantial degree of risk in pursuit of higher returns.

        **Q** **What does Buy and hold mean?**
        
        **A:** An investing strategy based on holding stocks and other assets in your portfolio for a long period of time, regardless of the ups and downs of the market.

        **Q: How does Robo Advisor work?**  
        
        **A:** It analyzes your preferences and suggests investment strategies to help you achieve your goals.

        **Q: Is this platform free?**  
        
        **A:** Yes, this platform is free and designed for educational purposes only.

        
        """)

    # Call-to-Action Footer
    if st.button("Start Building Your Portfolio"):
        st.session_state.page = "Questionnaire"  # Navigate to the Questionnaire page

# Questionnaire Tab
elif st.session_state.page == "Questionnaire":
    st.title("Let's Personalize Your Investment Journey")
    st.subheader("Answer these three simple questions to get started")

    # Questionnaire Inputs
    investment_amount = st.number_input("What amount would you like to start your investment journey with? (USD)", min_value=1000, step=500)
    
    # Investment Goal Question
    investment_goal = st.selectbox("What are you hoping to achieve with your investments?", ["Growth", "Income", "Stability", "Undecided"])
    
    # Friendly Note for "Undecided" Option
    st.markdown("*Not sure? No problem - select 'Undecided' and we’ll guide you!*")
    
    # Risk Tolerance Question
    risk_tolerance = st.selectbox("What is your risk tolerance?", ["Low", "Medium", "High"])

    
    # Explanation of Risk Tolerance 
    st.markdown("""
    **What is Risk Tolerance?**
    
    **Risk Tolerance** is how comfortable you are with the idea of losing money in the short term in hopes of making more money over time. If you have a low tolerance, you’d rather play it safe. If you have a high tolerance, you’re okay with seeing your investment go up and down because you’re aiming for bigger returns.
    - **Low**: You prefer safety and are willing to accept smaller, consistent returns with minimal risk.
    - **Medium**: You are open to taking some risks for moderate returns over time. Medium-risk is somewhere in between low and high risk.
    - **High**: You are comfortable with higher risks for the potential of greater returns. High-risk investments can go up a lot but also can drop suddenly.
    """)

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

# About Us Tab
elif st.session_state.page == "About Us":
    st.title("About Us")
    st.markdown("""
    Welcome to **Robo Advisor**, your intelligent investment portfolio generator.  

    **WHO ARE WE:**
    
    We are a team of passionate innovators dedicated to empowering individuals to take control of their financial futures.  

    **What Makes Us Unique?**
    - User-friendly design tailored for beginners.
    - Focused on education and guidance.
    - Transparent and reliable information.

    **OUR MISSION:**
    
    To provide beginners with the knowledge, tools, and confidence to navigate the world of investing and achieve financial independence.

    **OUR VISION:**
    
     To make financial literacy accessible to everyone by making investing accessible and comprehensible for everyone.
     making investing accessible and comprehensible for everyone.
    """)

    # Smaller DISCLAIMER Section
    st.markdown("""
    <style>
    .disclaimer-text {
        font-size: 0.8rem; /* Smaller font size */
        color: #555555;   /* Subtle color for readability */
        line-height: 1.5; /* Proper line spacing for readability */
    }
    </style>
    <div class="disclaimer-text">
        <strong>DISCLAIMER</strong><br><br>
        This Robo Advisor platform is developed as part of an academic project and is intended solely for educational purposes. 
        The information, insights, and recommendations provided by this platform are generated using historical data and basic financial models to help users learn about investing concepts.
        We are not professional financial advisors, and this platform should not be used as a substitute for professional financial advice. 
        Users are encouraged to consult a qualified financial advisor before making any investment decisions.<br><br>
        The creators of this platform assume no responsibility for financial outcomes or decisions made based on the information provided. 
        Use this platform at your own discretion and risk.<br><br>
    </div>
    """, unsafe_allow_html=True)


    st.markdown("""
        <style>
        .center-text {
            text-align: center;
            font-size: 0.9rem; /* Adjust font size to make it smaller */
            color: #555555;    /* Optional subtle color */
        }
        </style>
        <div class="center-text">
        <strong>Thank you for choosing Robo Advisor as your trusted companion on your investment journey!</strong>
        </div>
        """, unsafe_allow_html=True)
