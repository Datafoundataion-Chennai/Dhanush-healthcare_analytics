import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import logging
from datetime import datetime
from functools import lru_cache

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

# Cache for expensive queries
@st.cache_resource(ttl=3600)
def get_cached_client():
    return bigquery.Client()

# Function to Check if Column Exists with caching
@st.cache_data(ttl=3600)
def column_exists(table_name, column_name):
    query = f"""
    SELECT column_name 
    FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = '{table_name}' AND column_name = '{column_name}'
    """
    try:
        query_job = client.query(query)
        result = query_job.to_dataframe()
        return not result.empty
    except Exception as e:
        logger.error(f"Error checking column existence: {e}")
        return False

# Function to Execute SQL Query on BigQuery with caching
@st.cache_data(ttl=600)
def run_bigquery_query(query, write=False):
    try:
        logger.info(f"Executing query: {query}")
        query_job = client.query(query)
        if not write:
            df = query_job.to_dataframe()
            logger.info(f"Successfully loaded {len(df)} rows from query")
            return df
        else:
            query_job.result()
            logger.info("Write operation completed successfully")
            return True
    except Exception as e:
        st.error(f"Error executing query: {e}")
        logger.error(f"Query execution failed: {e}")
        return None

# User Authentication
def authenticate(username, password):
    # DEMO ONLY - Replace with proper authentication in production
    if username == "admin" and password == "admin123":
        return "admin"
    elif username == "user" and password == "user123":
        return "user"
    return None

# Login Section
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.role = None
    st.session_state.current_data = None

if not st.session_state.authenticated:
    st.title("Healthcare Analytics Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    
    if st.button("Login"):
        role = authenticate(username, password)
        if role:
            st.session_state.authenticated = True
            st.session_state.role = role
            st.success(f"Logged in as {role}")
            st.rerun()
        else:
            st.error("Invalid credentials")
    st.stop()

# Check permissions
def has_write_permission():
    return st.session_state.role == "admin"

# Function to Get Column Data Type with caching
@st.cache_data(ttl=3600)
def get_column_data_type(table_name, column_name):
    query = f"""
    SELECT data_type 
    FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = '{table_name}' AND column_name = '{column_name}'
    """
    result = run_bigquery_query(query)
    return result['data_type'].iloc[0] if result is not None and not result.empty else None

# Function to Get All Columns with caching
@st.cache_data(ttl=3600)
def get_all_columns(table_name):
    query = f"""
    SELECT column_name 
    FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = '{table_name}'
    """
    result = run_bigquery_query(query)
    return result['column_name'].tolist() if result is not None else []

# Function to Get Primary Key Column with caching
@st.cache_data(ttl=3600)
def get_primary_key(table_name):
    # Try to identify a likely primary key column
    possible_keys = ['id', 'ID', 'Id', 'record_id', 'RecordID', 'provider_id', 'patient_id', 'Provider_ID', 'Patient_ID']
    all_columns = get_all_columns(table_name)
    
    for key in possible_keys:
        if key in all_columns:
            return key
    
    # If no obvious key found, return the first column
    return all_columns[0] if all_columns else None

# Custom CSS for modern UI
st.markdown("""
    <style>
    /* Main app styling */
    .stApp {
        background-color: #f8f9fa;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    
    /* Header styling */
    .header-container {
        background: linear-gradient(135deg, #6e8efb, #a777e3);
        color: white;
        padding: 1rem;
        margin-bottom: 1rem;
        border-radius: 8px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .header-title {
        margin: 0;
        padding: 0;
        flex-grow: 1;
    }
    .user-info {
        display: flex;
        align-items: center;
        gap: 1rem;
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
    
    /* Logout button styling */
    .logout-btn {
        background-color: #ff4b4b !important;
        color: white !important;
        border: none !important;
    }
    .logout-btn:hover {
        background-color: #ff0000 !important;
    }
    
    /* Table styling */
    .stDataFrame {
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    /* Pagination styling */
    .pagination {
        display: flex;
        justify-content: center;
        align-items: center;
        margin-top: 1rem;
        gap: 0.5rem;
    }
    
    .page-info {
        margin: 0 1rem;
        font-weight: bold;
    }
    
    /* Warning box styling */
    .warning-box {
        background-color: #fff3cd;
        border-left: 5px solid #ffc107;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 4px;
    }
    
    /* Record selection styling */
    .record-selector {
        margin-bottom: 1rem;
    }
    
    /* Add some animation */
    @keyframes fadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
    }
    
    .fade-in {
        animation: fadeIn 0.5s ease-in;
    }
    </style>
""", unsafe_allow_html=True)

# App Header with working logout button
header_col1, header_col2 = st.columns([4, 1])
with header_col1:
    st.markdown(f"""
        <div class="header-container fade-in">
            <h1 class="header-title">Healthcare Provider Analytics Dashboard</h1>
        </div>
    """, unsafe_allow_html=True)
with header_col2:
    st.markdown(f"""
        <div class="user-info" style="margin-top: 1rem;">
            <div>Logged in as: <strong>{st.session_state.role}</strong></div>
        </div>
    """, unsafe_allow_html=True)
    if st.button("Logout", key="logout_button"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# Handle logout
if 'logout_button' in st.session_state and st.session_state.logout_button:
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

# Initialize session state for pagination
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1

# Track the current dataset to reset pagination when it changes
if 'current_dataset' not in st.session_state:
    st.session_state.current_dataset = None

# Sidebar Navigation with loading indicator
with st.sidebar:
    with st.spinner("Loading navigation..."):
        st.markdown('<div class="sidebar-title">Navigation</div>', unsafe_allow_html=True)
        selected_option = st.selectbox("Select Dataset", list(tables.keys()), key="dataset_select")
        logger.info(f"User selected dataset: {selected_option}")

        # Reset pagination if dataset changed
        if st.session_state.current_dataset != selected_option:
            st.session_state.current_page = 1
            st.session_state.current_dataset = selected_option
            st.session_state.current_data = None

        # Base table name
        table_name = tables[selected_option]

        # Dataset Filters
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.markdown("**Dataset Filters**")
        
        # Initialize base query parts
        base_query = f"SELECT * FROM `{dataset_id}.{table_name}`"
        where_conditions = []

        if selected_option == "Provider Productivity":
            if column_exists(table_name, "PROVIDER"):
                providers_query = f"SELECT DISTINCT PROVIDER FROM `{dataset_id}.{table_name}`"
                providers_df = run_bigquery_query(providers_query)
                if providers_df is not None:
                    provider_filter = st.multiselect(
                        "Filter by Provider", 
                        providers_df['PROVIDER'].tolist(),
                        key="provider_filter"
                    )
                    if provider_filter:
                        provider_list = ", ".join([f"'{p}'" for p in provider_filter])
                        where_conditions.append(f"PROVIDER IN ({provider_list})")

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
                        patient_list = ", ".join([f"'{p}'" for p in patient_filter])
                        where_conditions.append(f"`{patient_column}` IN ({patient_list})")

        elif selected_option == "CMS Data":
            if column_exists(table_name, "Facility Name"):
                facility_query = f"SELECT DISTINCT `Facility Name` FROM `{dataset_id}.{table_name}`"
                facility_df = run_bigquery_query(facility_query)
                if facility_df is not None:
                    facility_filter = st.multiselect(
                        "Filter by Facility Name",
                        facility_df['Facility Name'].tolist(),
                        key="facility_filter"
                    )
                    if facility_filter:
                        facility_list = ", ".join([f"'{f.replace("'", "''")}'" for f in facility_filter])
                        where_conditions.append(f"`Facility Name` IN ({facility_list})")
            
            columns_query = f"""
            SELECT column_name, data_type 
            FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS 
            WHERE table_name = '{table_name}'
            """
            columns_df = run_bigquery_query(columns_query)
            if columns_df is not None:
                filterable_columns = [col for col in columns_df['column_name'].tolist() 
                                    if not (column_exists(table_name, "Facility Name") and col == "Facility Name")]
                
                if filterable_columns:
                    filter_column = st.selectbox(
                        "Select Additional Column to Filter", 
                        filterable_columns,
                        key="cms_filter_col"
                    )
                    
                    col_data_type = columns_df[columns_df['column_name'] == filter_column]['data_type'].iloc[0]
                    
                    values_query = f"SELECT DISTINCT `{filter_column}` FROM `{dataset_id}.{table_name}`"
                    values_df = run_bigquery_query(values_query)
                    if values_df is not None:
                        selected_values = st.multiselect(
                            f"Filter by {filter_column}", 
                            values_df[filter_column].tolist(),
                            key="cms_filter_values"
                        )
                        if selected_values:
                            if col_data_type in ['INT64', 'FLOAT64', 'NUMERIC']:
                                value_list = ", ".join([str(v) for v in selected_values])
                            else:
                                value_list = ", ".join([f"'{str(v).replace("'", "''")}'" for v in selected_values])
                            
                            where_conditions.append(f"`{filter_column}` IN ({value_list})")
        
        st.markdown('</div>', unsafe_allow_html=True)

        # Construct final query only if we have conditions
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)

# Main Content Area with loading indicator
with st.container():
    with st.spinner("Loading data..."):
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
                search_conditions = " OR ".join([f"`{col}` LIKE '%{search_term.replace("'", "''")}%'" 
                                               for col in string_cols_df['column_name']])
                if where_conditions:
                    base_query += f" AND ({search_conditions})"
                else:
                    base_query += f" WHERE ({search_conditions})"

        # Execute the filtered query only if we don't have cached data
        if st.session_state.current_data is None or st.session_state.last_query_hash != hash(base_query):
            logger.info(f"Final query: {base_query}")
            df = run_bigquery_query(base_query)
            st.session_state.current_data = df
            st.session_state.last_query_hash = hash(base_query)
        else:
            df = st.session_state.current_data

        if df is not None and not df.empty:
            # Data Display with Tabs
            tab1, tab2 = st.tabs(["📋 Data Table", "📈 Visualizations"])
            
            with tab1:
                # Pagination settings
                items_per_page = 20
                total_pages = (len(df) // items_per_page) + (1 if len(df) % items_per_page else 0)
                
                # Calculate start and end indices for current page
                start_idx = (st.session_state.current_page - 1) * items_per_page
                end_idx = min(start_idx + items_per_page, len(df))
                
                # Display the current page of data
                st.dataframe(df.iloc[start_idx:end_idx], use_container_width=True)
                
                # Admin-only data editing functionality
                if has_write_permission():
                    st.markdown("### Admin Tools")
                    with st.expander("Add New Record"):
                        all_columns = get_all_columns(table_name)
                        if all_columns:
                            new_record = {}
                            for col in all_columns:
                                new_record[col] = st.text_input(f"Enter value for {col}")
                            
                            if st.button("Add Record"):
                                columns = ", ".join([f"`{col}`" for col in all_columns])
                                values = ", ".join([f"'{new_record[col]}'" if isinstance(new_record[col], str) else str(new_record[col]) for col in all_columns])
                                insert_query = f"INSERT INTO `{dataset_id}.{table_name}` ({columns}) VALUES ({values})"
                                if run_bigquery_query(insert_query, write=True):
                                    st.success("Record added successfully!")
                                    st.session_state.current_data = None  # Clear cache
                                    st.rerun()
                    
                    with st.expander("Update Records"):
                        st.info("Select a record to update")
                        
                        # Get primary key column
                        primary_key = get_primary_key(table_name)
                        if primary_key:
                            # Get all records with their primary keys for selection
                            id_query = f"SELECT `{primary_key}` FROM `{dataset_id}.{table_name}` ORDER BY `{primary_key}` LIMIT 1000"
                            id_df = run_bigquery_query(id_query)
                            
                            if id_df is not None and not id_df.empty:
                                selected_id = st.selectbox(
                                    f"Select record by {primary_key}",
                                    id_df[primary_key].tolist(),
                                    key="update_select"
                                )
                                
                                # Get the full record for the selected ID
                                record_query = f"SELECT * FROM `{dataset_id}.{table_name}` WHERE `{primary_key}` = "
                                if isinstance(selected_id, str):
                                    record_query += f"'{selected_id.replace("'", "''")}'"
                                else:
                                    record_query += str(selected_id)
                                
                                record_df = run_bigquery_query(record_query)
                                
                                if record_df is not None and not record_df.empty:
                                    st.markdown("### Current Record Values")
                                    current_values = record_df.iloc[0].to_dict()
                                    
                                    # Display current values and allow editing
                                    updated_values = {}
                                    for col in record_df.columns:
                                        if col == primary_key:
                                            st.text_input(f"{col} (Primary Key - cannot be changed)", value=current_values[col], disabled=True)
                                            updated_values[col] = current_values[col]
                                        else:
                                            col_type = get_column_data_type(table_name, col)
                                            if col_type in ['INT64', 'FLOAT64', 'NUMERIC']:
                                                updated_values[col] = st.number_input(col, value=float(current_values[col]) if pd.notna(current_values[col]) else 0.0)
                                            elif col_type == 'BOOLEAN':
                                                updated_values[col] = st.checkbox(col, value=bool(current_values[col]) if pd.notna(current_values[col]) else False)
                                            elif col_type == 'DATE':
                                                date_val = pd.to_datetime(current_values[col]) if pd.notna(current_values[col]) else None
                                                updated_values[col] = st.date_input(col, value=date_val)
                                            elif col_type == 'TIMESTAMP':
                                                datetime_val = pd.to_datetime(current_values[col]) if pd.notna(current_values[col]) else None
                                                updated_values[col] = st.datetime_input(col, value=datetime_val)
                                            else:
                                                # Default to text input for strings and other types
                                                updated_values[col] = st.text_input(col, value=str(current_values[col]) if pd.notna(current_values[col]) else "")
                                    
                                    if st.button("Update Record"):
                                        # Build SET clause for UPDATE
                                        set_clauses = []
                                        for col in updated_values:
                                            if col == primary_key:
                                                continue
                                            
                                            val = updated_values[col]
                                            if isinstance(val, str):
                                                set_clauses.append(f"`{col}` = '{val.replace("'", "''")}'")
                                            elif pd.isna(val) or val is None:
                                                set_clauses.append(f"`{col}` = NULL")
                                            else:
                                                set_clauses.append(f"`{col}` = {val}")
                                        
                                        # Build WHERE condition for primary key
                                        where_condition = f"`{primary_key}` = "
                                        if isinstance(updated_values[primary_key], str):
                                            where_condition += f"'{updated_values[primary_key].replace("'", "''")}'"
                                        else:
                                            where_condition += str(updated_values[primary_key])
                                        
                                        update_query = f"""
                                        UPDATE `{dataset_id}.{table_name}`
                                        SET {", ".join(set_clauses)}
                                        WHERE {where_condition}
                                        """
                                        
                                        if run_bigquery_query(update_query, write=True):
                                            st.success("Record updated successfully!")
                                            st.session_state.current_data = None  # Clear cache
                                            st.rerun()
                                        else:
                                            st.error("Failed to update record")
                                else:
                                    st.warning("No record found for the selected ID")
                            else:
                                st.warning(f"No records found in the table")
                        else:
                            st.error("Could not identify a primary key column for updates")
                    
                    with st.expander("Delete Records"):
                        st.markdown('<div class="warning-box">⚠️ Warning: Deleting records cannot be undone. Please be certain before proceeding.</div>', unsafe_allow_html=True)
                        
                        # Get primary key column
                        primary_key = get_primary_key(table_name)
                        if primary_key:
                            # Get all records with their primary keys for selection
                            id_query = f"SELECT `{primary_key}` FROM `{dataset_id}.{table_name}` ORDER BY `{primary_key}` LIMIT 1000"
                            id_df = run_bigquery_query(id_query)
                            
                            if id_df is not None and not id_df.empty:
                                selected_ids = st.multiselect(
                                    f"Select records to delete by {primary_key}",
                                    id_df[primary_key].tolist(),
                                    key="delete_select"
                                )
                                
                                if selected_ids:
                                    st.warning(f"You have selected {len(selected_ids)} record(s) for deletion")
                                    
                                    # Show a preview of the records to be deleted
                                    preview_query = f"""
                                    SELECT * FROM `{dataset_id}.{table_name}`
                                    WHERE `{primary_key}` IN ("""
                                    
                                    # Handle different data types for the IDs
                                    id_list = []
                                    for id_val in selected_ids:
                                        if isinstance(id_val, str):
                                            id_list.append(f"'{id_val.replace("'", "''")}'")
                                        else:
                                            id_list.append(str(id_val))
                                    
                                    preview_query += ", ".join(id_list) + ") LIMIT 10"
                                    preview_df = run_bigquery_query(preview_query)
                                    
                                    if preview_df is not None and not preview_df.empty:
                                        st.write("Preview of records to be deleted:")
                                        st.dataframe(preview_df)
                                        
                                        confirm = st.checkbox("I confirm I want to delete these records", key="delete_confirm")
                                        
                                        if confirm and st.button("Delete Selected Records", type="primary"):
                                            delete_query = f"""
                                            DELETE FROM `{dataset_id}.{table_name}`
                                            WHERE `{primary_key}` IN ({", ".join(id_list)})
                                            """
                                            
                                            if run_bigquery_query(delete_query, write=True):
                                                st.success(f"Successfully deleted {len(selected_ids)} record(s)")
                                                st.session_state.current_data = None  # Clear cache
                                                st.rerun()
                                            else:
                                                st.error("Failed to delete records")
                                    else:
                                        st.warning("No records found for the selected IDs")
                                else:
                                    st.info("Please select records to delete")
                            else:
                                st.warning(f"No records found in the table")
                        else:
                            st.error("Could not identify a primary key column for deletion")
                
                # Pagination controls at the bottom
                col1, col2, col3 = st.columns([1, 2, 1])
                with col1:
                    if st.button("⏮ Previous Page", disabled=st.session_state.current_page <= 1):
                        st.session_state.current_page -= 1
                with col2:
                    st.markdown(f"""
                    <div class="pagination">
                        <span class="page-info">Page {st.session_state.current_page} of {total_pages}</span>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    if st.button("Next Page ⏭", disabled=st.session_state.current_page >= total_pages):
                        st.session_state.current_page += 1

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
                    viz_type = st.selectbox(
                        "Select Visualization Type",
                        ["Bar Chart", "Line Chart", "Scatter Plot", "Histogram", "Pie Chart"],
                        key="viz_type"
                    )
                    
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
                        """
                        if where_conditions:
                            bar_query += " WHERE " + " AND ".join(where_conditions)
                        bar_query += f" ORDER BY `{bar_col}` DESC LIMIT 10"
                        
                        bar_df = run_bigquery_query(bar_query)
                        if bar_df is not None:
                            fig, ax = plt.subplots(figsize=(10, 6))
                            sns.barplot(x=bar_df[index_col], y=bar_df[bar_col], palette="viridis", ax=ax)
                            plt.xticks(rotation=45)
                            plt.title(f"Top 10 by {bar_col}")
                            st.pyplot(fig)
                    
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