import pandas as pd
from google.cloud import bigquery
import os

# Define absolute paths to the CSV files
base_dir = 'E:/HealthCare Project/data/transformed/'
patients_file = os.path.join(base_dir, 'patients_data_cleaned.csv')
encounters_file = os.path.join(base_dir, 'encounters_data_cleaned.csv')
cms_file = os.path.join(base_dir, 'cms_data_cleaned.csv')

# Check if files exist before reading
if not os.path.exists(patients_file):
    raise FileNotFoundError(f"File not found: {patients_file}")
if not os.path.exists(encounters_file):
    raise FileNotFoundError(f"File not found: {encounters_file}")
if not os.path.exists(cms_file):
    raise FileNotFoundError(f"File not found: {cms_file}")

# Read CSV files
patients_data = pd.read_csv(patients_file)
encounters_data = pd.read_csv(encounters_file)
cms_data = pd.read_csv(cms_file)

# BigQuery setup
dataset_id = 'healthcare_analytics'
tables = {
    'patients_data': patients_data,
    'encounters_data': encounters_data,
    'cms_data': cms_data,
}

client = bigquery.Client()
dataset_ref = client.dataset(dataset_id)

# Create dataset if it doesn't exist
try:
    client.get_dataset(dataset_ref)
    print(f"Dataset {dataset_id} Already Exists.")
except Exception:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = 'US'
    client.create_dataset(dataset)
    print(f"Dataset {dataset_id} Created.")

# Upload tables to BigQuery
for table_name, df in tables.items():
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  
    print(f"Table {table_name} Created and Data Uploaded.")