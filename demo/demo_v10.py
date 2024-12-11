import streamlit as st
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import pickle
import plotly.express as px

# Load the trained K-Means model
model_filename = "kmeans_model.pkl"
with open(model_filename, "rb") as file:
    kmeans = pickle.load(file)

# Streamlit App Configuration
st.set_page_config(page_title="Robo Advisor - Portfolio Recommendation", layout="wide")

# Initialize session state for pagination
if "current_step" not in st.session_state:
    st.session_state.current_step = 0  # Start at the landing page (Step 0)

def next_step():
    st.session_state.current_step += 1

def prev_step():
    st.session_state.current_step -= 1

def button_navigation():
    """Render horizontally aligned Previous and Next buttons."""
    col1, col2, _ = st.columns([1, 1, 6])
    with col1:
        if st.button("Previous") and st.session_state.current_step > 0:
            prev_step()
    with col2:
        if st.button("Next") and st.session_state.current_step < total_steps - 1:
            next_step()

# Total steps (including landing page)
total_steps = 4

# Custom CSS for progress bar and layout
st.markdown(
    f"""
    <style>
    .progress-bar {{
        position: relative;
        height: 20px;
        border-radius: 10px;
        background-color: #f0f0f0;
        margin: 20px 0;
    }}
    .progress-bar-fill {{
        height: 100%;
        border-radius: 10px;
        background-color: #2a9d8f;
        width: {st.session_state.current_step / (total_steps - 1) * 100}%;
        transition: width 0.3s ease-in-out;
    }}
    .block-container {{
        padding: 2rem 2rem;
        margin: auto;
        max-width: 800px;
        background-color: #ffffff;
        border-radius: 10px;
        box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1);
    }}
    .stButton > button {{
        padding: 10px 20px;
        border: none;
        background-color: #007BFF;
        color: white;
        border-radius: 5px;
        cursor: pointer;
    }}
    .stButton > button:hover {{
        background-color: #0056b3;
    }}
    </style>
    <div class="progress-bar">
        <div class="progress-bar-fill"></div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Step 0: Landing Page
if st.session_state.current_step == 0:
    st.title("Welcome to the Robo Advisor")
    st.markdown(
        """
        #### Personalized Portfolio Recommendations at Your Fingertips
        This application helps you tailor investment portfolios based on your preferences and risk tolerance.
        
        **Features**:
        - Gather user information through a questionnaire.
        - Use machine learning to recommend investment portfolios.
        - Visualize portfolio clusters and insights.
        
        Click the **Get Started** button below to begin!
        """
    )
    if st.button("Get Started"):
        next_step()

# Step 1: Questionnaire
elif st.session_state.current_step == 1:
    st.header("Step 1: Questionnaire")
    st.write("Please provide the necessary details to tailor your portfolio recommendation.")

    # Input form
    with st.form("questionnaire_form"):
        user_age = st.slider("Enter Your Age", min_value=18, max_value=100, value=30)
        investment_amount = st.number_input(
            "Investment Amount (in $)", min_value=1000, step=1000, value=100000
        )
        investment_duration = st.number_input(
            "Investment Duration (in Years)", min_value=1, max_value=50, value=20
        )
        submitted = st.form_submit_button("Next")
        if submitted:
            st.session_state.user_age = user_age
            st.session_state.investment_amount = investment_amount
            st.session_state.investment_duration = investment_duration
            next_step()

# Step 2: Suggested Portfolio
elif st.session_state.current_step == 2:
    st.header("Step 2: Suggested Portfolio")
    st.write("Based on your input, here are the suggested portfolios grouped by their clusters.")

    # Dummy data for the portfolio
    data = pd.DataFrame(
        {
            "Beta": np.random.rand(100),
            "Alpha": np.random.rand(100),
            "Dividend_Yield": np.random.rand(100),
            "10-Day Volatility": np.random.rand(100),
        }
    )

    # Add user inputs to the data
    data["User_Age"] = st.session_state.user_age
    data["Investment_Amount"] = st.session_state.investment_amount
    data["Investment_Duration"] = st.session_state.investment_duration

    # Clustering
    scaler = StandardScaler()
    required_features = [
        "Beta",
        "Alpha",
        "Dividend_Yield",
        "10-Day Volatility",
        "User_Age",
        "Investment_Amount",
        "Investment_Duration",
    ]
    data_scaled = scaler.fit_transform(data[required_features])
    data["Cluster"] = kmeans.predict(data_scaled)

    # Show recommendations
    recommended_portfolio = data.groupby("Cluster").head(5)
    st.dataframe(recommended_portfolio[["Beta", "Alpha", "Cluster"]])

    # Visualization
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

    # Navigation buttons
    button_navigation()

# Step 3: Data Insights
elif st.session_state.current_step == 3:
    st.header("Step 3: Data Insights")
    st.write("Explore insights from the dataset, including statistical summaries and visualizations.")

    # Summary Stats
    st.subheader("Statistical Summary")
    st.write(data.describe())

    # Visualization
    fig = px.histogram(
        data,
        x="Cluster",
        title="Distribution of Clusters",
        labels={"Cluster": "Cluster Label"},
    )
    st.plotly_chart(fig)

    # Navigation buttons
    button_navigation()
