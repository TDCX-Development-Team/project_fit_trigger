import pandas as pd
from google.cloud import bigquery
import re
from datetime import datetime
from config import PROJECT_ID, DATASET_NAME, TABLE_NAME

def load_excel_to_dataframe(file_path):
    """Load an Excel file into a Pandas DataFrame."""
    # Load the Excel file and set the header to the second row (index 1)
    df = pd.read_excel(file_path, header=1)  # Read the file once with header on the second row
    # Clean up the column names
    df.columns = df.columns.str.strip()  # Strip whitespace from column names
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)  # Replace invalid characters
    df.columns = df.columns.str.replace(r'_{2,}', '_', regex=True)  # Replace multiple underscores
    df.columns = df.columns.str.strip('_')  # Strip leading/trailing underscores
    df['start_date'] = datetime.now().date()  # Add start_date column
    df['end_date'] = None  # Add end_date column
    df = df.applymap(lambda x: str(x).strip() if isinstance(x, str) else x)  # Strip strings
    df['EMP_ID'] = df['EMP_ID'].astype(str).str.replace(r'-', '', regex=True)  # Clean EMP_ID
    df['EMP_ID'] = pd.to_numeric(df['EMP_ID'], errors='coerce')  # Convert EMP_ID to numeric
    df = df.applymap(lambda x: str(x) if not pd.isna(x) else '')  
    return df

def load_dataframe_to_bigquery(df):
    """Load the DataFrame to BigQuery."""
    bigquery_client = bigquery.Client()
    table_id = f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}'

    try:
        job_config = bigquery.LoadJobConfig(autodetect=True)
        job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  
        print(f'Loaded {job.output_rows} rows into {table_id}.')
        return True
    except Exception as e:
        print(f'An error occurred while loading DataFrame to BigQuery: {e}')
        return False
