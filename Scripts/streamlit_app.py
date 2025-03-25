import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import logging
from datetime import datetime

# Set page config must be first Streamlit command
st.set_page_config(
    page_title="Healthcare Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configure logging
logging.basicConfig(
    filename="healthcare_dashboard.log", 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

logger.info("Healthcare Provider Analytics Dashboard started successfully!")

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"E:\HealthCare Project\buoyant-cargo-454110-t7-fca2b1116372.json"

# Validate credentials
if not os.path.exists(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]):
    st.error(f"Error: Credentials file not found at {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}.")
    logger.error(f"Credentials file not found at {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}.")
    st.stop()

# Initialize BigQuery client
try:
    client = bigquery.Client()
    logger.info("BigQuery client initialized successfully.")
except Exception as e:
    st.error(f"Error initializing BigQuery client: {e}")
    logger.error(f"BigQuery client initialization failed: {e}")
    st.stop()

# Dataset and Table Names
dataset_id = 'healthcare_analytics'
tables = {
    "Provider Productivity": "provider_productivity",
    "Appointment Analytics": "appointment_analytics",
    "CMS Data": "cms_data"
}

# Function to Execute SQL Query on BigQuery
def run_bigquery_query(query):
    try:
        logger.info(f"Executing query: {query}")
        query_job = client.query(query)
        df = query_job.to_dataframe()
        logger.info(f"Successfully loaded {len(df)} rows from query")
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        logger.error(f"Query execution failed: {e}")
        return None

# Function to Check if Column Exists
def column_exists(table_name, column_name):
    query = f"""
    SELECT column_name 
    FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = '{table_name}' AND column_name = '{column_name}'
    """
    result = run_bigquery_query(query)
    return result is not None and not result.empty

# Function to Get All Columns
def get_all_columns(table_name):
    query = f"""
    SELECT column_name 
    FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = '{table_name}'
    """
    result = run_bigquery_query(query)
    return result['column_name'].tolist() if result is not None else []

# Custom CSS for modern UI
st.markdown("""
    <style>
    /* Main app styling */
    .stApp {
        background-color: #f8f9fa;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    
    /* Header styling */
    .header {
        background: linear-gradient(135deg, #6e8efb, #a777e3);
        color: white;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #4b6cb7 0%, #182848 100%) !important;
        color: white !important;
    }
    
    .sidebar-title {
        color: white !important;
        font-weight: 700;
        font-size: 1.2rem;
        margin-bottom: 0.5rem;
    }
    
    .sidebar-section {
        background-color: rgba(255,255,255,0.1);
        border-radius: 8px;
        padding: 0.6rem;
        margin-bottom: 0.8rem;
    }
    
    /* Button styling */
    .stButton>button {
        border-radius: 20px;
        background: linear-gradient(135deg, #6e8efb, #a777e3);
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: 600;
    }
    
    /* Table styling */
    .stDataFrame {
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)

# App Header
st.markdown("""
    <div class="header">
        <h1 style="color:white; margin:0; padding:0;">Healthcare Provider Analytics Dashboard</h1>
    </div>
""", unsafe_allow_html=True)

# Sidebar Navigation
with st.sidebar:
    st.markdown('<div class="sidebar-title">Navigation</div>', unsafe_allow_html=True)
    selected_option = st.selectbox("Select Dataset", list(tables.keys()), key="dataset_select")
    logger.info(f"User selected dataset: {selected_option}")

    # Base table name
    table_name = tables[selected_option]

    # Dataset Filters - moved directly under navigation without extra space
    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    st.markdown("**Dataset Filters**")
    
    # Base SQL Query
    base_query = f"SELECT * FROM `{dataset_id}.{table_name}` WHERE 1=1"

    if selected_option == "Provider Productivity":
        if column_exists(table_name, "PROVIDER"):
            providers_query = f"SELECT DISTINCT `PROVIDER` FROM `{dataset_id}.{table_name}`"
            providers_df = run_bigquery_query(providers_query)
            if providers_df is not None:
                provider_filter = st.multiselect(
                    "Filter by Provider", 
                    providers_df['PROVIDER'].tolist(),
                    key="provider_filter"
                )
                if provider_filter:
                    provider_list = ",".join([f"'{p}'" for p in provider_filter])
                    base_query += f" AND `PROVIDER` IN ({provider_list})"

    elif selected_option == "Appointment Analytics":
        patient_column = None
        possible_patient_columns = ["PATIENT_ID", "PatientID", "patient_id", "PATIENTID", "Patient ID"]
        for col in possible_patient_columns:
            if column_exists(table_name, col):
                patient_column = col
                break
        
        if patient_column:
            patient_query = f"SELECT DISTINCT `{patient_column}` FROM `{dataset_id}.{table_name}`"
            patients_df = run_bigquery_query(patient_query)
            if patients_df is not None:
                patient_filter = st.multiselect(
                    f"Filter by {patient_column}", 
                    patients_df[patient_column].tolist(),
                    key="patient_filter"
                )
                if patient_filter:
                    patient_list = ",".join([f"'{p}'" for p in patient_filter])
                    base_query += f" AND `{patient_column}` IN ({patient_list})"

    elif selected_option == "CMS Data":
        columns_query = f"SELECT column_name FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table_name}'"
        columns_df = run_bigquery_query(columns_query)
        if columns_df is not None:
            filter_column = st.selectbox(
                "Select Column to Filter", 
                columns_df['column_name'].tolist(),
                key="cms_filter_col"
            )
            values_query = f"SELECT DISTINCT `{filter_column}` FROM `{dataset_id}.{table_name}`"
            values_df = run_bigquery_query(values_query)
            if values_df is not None:
                selected_values = st.multiselect(
                    f"Filter by {filter_column}", 
                    values_df[filter_column].tolist(),
                    key="cms_filter_values"
                )
                if selected_values:
                    value_list = ",".join([f"'{v}'" for v in selected_values])
                    base_query += f" AND `{filter_column}` IN ({value_list})"
    st.markdown('</div>', unsafe_allow_html=True)

# Main Content Area
with st.container():
    # Search Bar at Top
    search_term = st.text_input("Search records:", key="main_search")
    
    if search_term:
        string_cols_query = f"""
        SELECT column_name 
        FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS 
        WHERE table_name = '{table_name}' AND data_type IN ('STRING')
        """
        string_cols_df = run_bigquery_query(string_cols_query)
        if string_cols_df is not None and not string_cols_df.empty:
            search_conditions = " OR ".join([f"`{col}` LIKE '%{search_term}%'" for col in string_cols_df['column_name']])
            base_query += f" AND ({search_conditions})"

    # Execute the filtered query
    df = run_bigquery_query(base_query)

    if df is not None and not df.empty:
        # Data Display with Tabs
        tab1, tab2 = st.tabs(["📋 Data Table", "📈 Visualizations"])
        
        with tab1:
            st.dataframe(df, use_container_width=True)

        with tab2:
            st.markdown("### Data Visualizations")
            
            numeric_cols_query = f"""
            SELECT column_name 
            FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS 
            WHERE table_name = '{table_name}' AND data_type IN ('INT64', 'FLOAT64', 'NUMERIC')
            """
            numeric_cols_df = run_bigquery_query(numeric_cols_query)
            numeric_cols = numeric_cols_df['column_name'].tolist() if numeric_cols_df is not None else []
            
            all_cols = get_all_columns(table_name)
            categorical_cols = [col for col in all_cols if col not in numeric_cols]
            
            if numeric_cols:
                # Visualization Type Selector
                viz_type = st.selectbox(
                    "Select Visualization Type",
                    ["Bar Chart", "Line Chart", "Scatter Plot", "Histogram", "Pie Chart"],
                    key="viz_type"
                )
                
                # Bar Chart
                if viz_type == "Bar Chart":
                    st.markdown("#### Top Performers")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        bar_col = st.selectbox(
                            "Select Metric", 
                            numeric_cols,
                            key="bar_col"
                        )
                    
                    with col2:
                        index_col = st.selectbox(
                            "Select Category", 
                            categorical_cols,
                            key="index_col"
                        )
                    
                    bar_query = f"""
                    SELECT `{index_col}`, `{bar_col}` 
                    FROM `{dataset_id}.{table_name}` 
                    {base_query.split('WHERE 1=1')[1]} 
                    ORDER BY `{bar_col}` DESC 
                    LIMIT 10
                    """
                    bar_df = run_bigquery_query(bar_query)
                    if bar_df is not None:
                        fig, ax = plt.subplots(figsize=(10, 6))
                        sns.barplot(x=bar_df[index_col], y=bar_df[bar_col], palette="viridis", ax=ax)
                        plt.xticks(rotation=45)
                        plt.title(f"Top 10 by {bar_col}")
                        st.pyplot(fig)
                
                # Line Chart
                elif viz_type == "Line Chart":
                    st.markdown("#### Trend Analysis")
                    line_col = st.selectbox(
                        "Select Metric to Plot", 
                        numeric_cols,
                        key="line_col"
                    )
                    
                    if categorical_cols:
                        group_col = st.selectbox(
                            "Group by (optional)", 
                            [None] + categorical_cols,
                            key="group_col"
                        )
                    
                    if group_col:
                        line_df = df.groupby(group_col)[line_col].mean().reset_index()
                        fig = px.line(line_df, x=group_col, y=line_col, title=f"{line_col} by {group_col}")
                    else:
                        fig = px.line(df, y=line_col, title=f"Trend of {line_col}")
                    st.plotly_chart(fig, use_container_width=True)
                
                # Scatter Plot
                elif viz_type == "Scatter Plot":
                    st.markdown("#### Correlation Analysis")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        x_col = st.selectbox(
                            "X-axis Variable", 
                            numeric_cols,
                            key="x_col"
                        )
                    
                    with col2:
                        y_col = st.selectbox(
                            "Y-axis Variable", 
                            numeric_cols,
                            index=1 if len(numeric_cols) > 1 else 0,
                            key="y_col"
                        )
                    
                    if categorical_cols:
                        color_col = st.selectbox(
                            "Color by (optional)", 
                            [None] + categorical_cols,
                            key="color_col"
                        )
                    
                    fig = px.scatter(
                        df, 
                        x=x_col, 
                        y=y_col, 
                        color=color_col,
                        title=f"{x_col} vs {y_col}"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Histogram
                elif viz_type == "Histogram":
                    st.markdown("#### Distribution Analysis")
                    hist_col = st.selectbox(
                        "Select Variable", 
                        numeric_cols,
                        key="hist_col"
                    )
                    
                    bins = st.slider(
                        "Number of Bins", 
                        min_value=5, 
                        max_value=100, 
                        value=20,
                        key="bins"
                    )
                    
                    fig = px.histogram(
                        df, 
                        x=hist_col, 
                        nbins=bins,
                        title=f"Distribution of {hist_col}"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Pie Chart
                elif viz_type == "Pie Chart":
                    st.markdown("#### Composition Analysis")
                    if categorical_cols:
                        pie_col = st.selectbox(
                            "Select Categorical Variable", 
                            categorical_cols,
                            key="pie_col"
                        )
                        
                        value_col = st.selectbox(
                            "Select Value Metric", 
                            numeric_cols,
                            key="value_col"
                        )
                        
                        pie_df = df.groupby(pie_col)[value_col].sum().reset_index()
                        fig = px.pie(
                            pie_df, 
                            names=pie_col, 
                            values=value_col,
                            title=f"Composition by {pie_col}"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No categorical columns available for pie chart")

    else:
        st.error("⚠ No data available or incorrect dataset selection!")

# Footer
st.markdown(f"""
<div style="text-align: center; margin-top: 2rem; color: #666;">
    <p>Healthcare Analytics Dashboard • {datetime.now().strftime('%Y-%m-%d')}</p>
</div>
""", unsafe_allow_html=True)

logger.info("Dashboard execution completed successfully.")