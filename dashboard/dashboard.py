import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime

MOOSE_API_URL = "http://localhost:4000/consumption"

# Configure the page
st.set_page_config(
    page_title="GitHub Analytics Dashboard",
    page_icon="⭐",
    layout="wide"
)

# Add title and description
st.title("GitHub Analytics Dashboard")
st.markdown("View your repository stars and language statistics")

# Sidebar for configuration
with st.sidebar:
    github_username = st.text_input("GitHub Username")
    # api_key = st.text_input("API Key", type="password")
    
    if st.button("Fetch Data"):
        if github_username:
            # API endpoints
            stargazers_url = f"{MOOSE_API_URL}/ranked_stargazers"
            languages_url = f"{MOOSE_API_URL}/ranked_languages"
            
            headers = {
                "Content-Type": "application/json"
            }
            
            try:
                # Fetch data from both APIs
                stargazers_response = requests.get(stargazers_url, headers=headers)
                languages_response = requests.get(languages_url, headers=headers)
                
                if stargazers_response.status_code == 200 and languages_response.status_code == 200:
                    # Store the responses in session state
                    st.session_state.stargazers_data = stargazers_response.json()
                    st.session_state.languages_data = languages_response.json()
                    st.success("Data fetched successfully!")
                else:
                    st.error("Failed to fetch data. Please check your credentials.")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

# Main content area
if 'stargazers_data' in st.session_state and 'languages_data' in st.session_state:
    # Create two columns for the dashboard
    col1, col2 = st.columns(2)
    
    with col1:
        # Stars over time chart
        st.subheader("Stars Over Time")
        stars_df = pd.DataFrame(st.session_state.stargazers_data['developer_login'])
        stars_df['date'] = pd.to_datetime(stars_df['date'])
        
        fig_stars = px.line(
            stars_df,
            x='date',
            y='count',
            title='Repository Stars Over Time'
        )
        st.plotly_chart(fig_stars, use_container_width=True)
        
        # Top stargazers
        st.subheader("Top Stargazers")
        stargazers_df = pd.DataFrame(st.session_state.stargazers_data['top_stargazers'])
        st.dataframe(
            stargazers_df[['username', 'followers', 'public_repos']],
            use_container_width=True
        )
    
    with col2:
        # Language distribution pie chart
        st.subheader("Language Distribution")
        languages_df = pd.DataFrame(st.session_state.languages_data['languages'])
        
        fig_languages = px.pie(
            languages_df,
            values='percentage',
            names='language',
            title='Repository Language Distribution'
        )
        st.plotly_chart(fig_languages, use_container_width=True)
        
        # Language stats table
        st.subheader("Language Statistics")
        st.dataframe(
            languages_df[['language', 'lines_of_code', 'percentage']],
            use_container_width=True
        )

    # Additional metrics
    st.subheader("Key Metrics")
    metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
    
    with metrics_col1:
        st.metric(
            "Total Stars",
            st.session_state.stargazers_data['total_stars']
        )
    
    with metrics_col2:
        st.metric(
            "Total Repositories",
            st.session_state.stargazers_data['total_repos']
        )
    
    with metrics_col3:
        st.metric(
            "Languages Used",
            len(st.session_state.languages_data['languages'])
        )

else:
    st.info("Please enter your GitHub username and API key in the sidebar to fetch data.")
