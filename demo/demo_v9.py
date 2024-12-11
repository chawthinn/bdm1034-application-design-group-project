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

# Initialize session state for tab navigation
if "active_tab" not in st.session_state:
    st.session_state.active_tab = "📝 Questionnaire"

# Tab navigation logic
if st.session_state.active_tab == "📝 Questionnaire":
    tab1, tab2, tab3 = st.tabs(["📝 Questionnaire", "📊 Suggested Portfolio", "📈 Data Insights"])
elif st.session_state.active_tab == "📊 Suggested Portfolio":
    tab2, tab1, tab3 = st.tabs(["📊 Suggested Portfolio", "📝 Questionnaire", "📈 Data Insights"])
else:
    tab3, tab1, tab2 = st.tabs(["📈 Data Insights", "📝 Questionnaire", "📊 Suggested Portfolio"])

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
                st.session_state.active_tab = "📊 Suggested Portfolio"  # Change to the second tab

# Load a dummy dataset
data = pd.DataFrame(
    {
        "Beta": np.random.rand(100),
        "Alpha": np.random.rand(100),
        "Dividend_Yield": np.random.rand(100),
        "10-Day Volatility": np.random.rand(100),
    }
)

# Add synthetic user features
data["User_Age"] = user_age
data["Investment_Amount"] = investment_amount
data["Investment_Duration"] = investment_duration

# Features to be used for clustering
required_features = [
    "Beta",
    "Alpha",
    "Dividend_Yield",
    "10-Day Volatility",
    "User_Age",
    "Investment_Amount",
    "Investment_Duration",
]

# Preprocess Data
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[required_features])

# Predict Clusters
data["Cluster"] = kmeans.predict(data_scaled)

# Tab 2: Suggested Portfolio
with tab2:
    st.header("Suggested Portfolio")
    st.write(
        "Based on your input, here are the suggested portfolios grouped by their clusters."
    )
    recommended_portfolio = data.groupby("Cluster").head(5)

    with st.container():
        st.markdown(
            """
            <div class="portfolio-section">
                <h2>Portfolio Recommendations</h2>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.dataframe(recommended_portfolio[["Beta", "Alpha", "Cluster"]])

        fig = px.scatter(
            data,
            x="Beta",
            y="Alpha",
            color="Cluster",
            title="Portfolio Distribution by Clusters",
            labels={"Beta": "Beta", "Alpha": "Alpha"},
            hover_data=["Dividend_Yield", "10-Day Volatility"],
        )
        st.plotly_chart(fig)

# Tab 3: Data Insights
with tab3:
    st.header("Data Insights")
    st.write(
        "Explore insights from the dataset, including statistical summaries and visualizations."
    )

    with st.container():
        st.markdown(
            """
            <div class="insights-section">
                <h2>Statistical Summary</h2>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.write(data.describe())

        fig = px.histogram(
            data,
            x="Cluster",
            title="Distribution of Clusters",
            labels={"Cluster": "Cluster Label"},
        )
        st.plotly_chart(fig)
