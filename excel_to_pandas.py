import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from config import PROJECT_ID, DATASET_NAME, TABLE_NAME
import logging
import fsspec

def load_excel_to_dataframe(file_path):
    """Load an Excel file into a Pandas DataFrame."""
    try:
        logging.info(f"Loading Excel file from: {file_path}")
        df = pd.read_excel(file_path, sheet_name='Roster ALO', usecols='A:X', header=0)

        # Clean and prepare the columns
        df.columns = df.columns.str.strip().str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)\
            .str.replace(r'_{2,}', '_', regex=True).str.strip('_').str.lower()  # Convert column names to lowercase

        # Transform specific columns to string
        columns_to_convert = ['emp_id', 'wave', 'national_id', 'address', 
                              'barrio_localidad', 'natterbox', 'phone_number']

        # Convert the specified columns to string
        df[columns_to_convert] = df[columns_to_convert].fillna('').astype('string')
        df['tenure'] = pd.to_numeric(df['tenure'], errors='coerce')  # Convert to numeric, set invalid entries to NaN
        df['tenure'].fillna(0, inplace=True)  # Replace NaN with 0 (or any default value)
        df['tenure'] = df['tenure'].astype('int64')  # Convert to int64

        #df['birthday'] = (pd.to_datetime(df['birthday'].replace('-', pd.NaT), format='%d-%m-%Y', errors='coerce').dt.strftime('%d/%m/%Y'))
        # Log data types after conversion to ensure they are correct
        logging.info(f"Data types after string conversion: {df.dtypes}")

        # Handle date columns separately
        date_columns = ['contract_end_date', 'date_of_hire', 'go_live', 
                        'termination_date', 'start_date', 'end_date', 'birthday']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].replace('-', pd.NaT)  # Replace '-' with NaT
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date  # Convert to date (YYYY-MM-DD format)

                # Log the conversion results
                logging.info(f"Processed date column: {col}, resulting data types: {df[col].dtypes}")
        

        # Ensure 'start_date' and 'end_date' are present and filled with current date if missing
        current_date = pd.to_datetime(datetime.now().date()).date()
        if 'start_date' not in df.columns:
            df['start_date'] = current_date
        if 'end_date' not in df.columns:
            df['end_date'] = current_date


        # Final logging of DataFrame shape and types
        logging.info(f"Loaded DataFrame with {len(df)} rows and columns: {df.columns.tolist()}")
        logging.info("Final DataFrame dtypes:")
        logging.info(df.dtypes)

        return df

    except Exception as e:
        logging.error(f"Error loading Excel file '{file_path}' into DataFrame: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error


TABLE_SCHEMA = [
    bigquery.SchemaField("emp_id", "STRING"),
    bigquery.SchemaField("site", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("role", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("leader", "STRING"),
    bigquery.SchemaField("manager", "STRING"),
    bigquery.SchemaField("work_email", "STRING"),
    bigquery.SchemaField("wave", "STRING"),
    bigquery.SchemaField("alo_credential_user_name", "STRING"),
    bigquery.SchemaField("date_of_hire", "DATE"),
    bigquery.SchemaField("termination_date", "DATE"),
    bigquery.SchemaField("go_live", "DATE"),
    bigquery.SchemaField("tenure", "INT64"),
    bigquery.SchemaField("contract_type", "STRING"),
    bigquery.SchemaField("contract_end_date", "DATE"),
    bigquery.SchemaField("flash_card_user", "STRING"),
    bigquery.SchemaField("national_id", "STRING"),
    bigquery.SchemaField("personal_email", "STRING"),
    bigquery.SchemaField("birthday", "DATE"),
    bigquery.SchemaField("address", "STRING"),
    bigquery.SchemaField("barrio_localidad", "STRING"),
    bigquery.SchemaField("phone_number", "STRING"),
    bigquery.SchemaField("natterbox", "STRING"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("end_date", "DATE"),
]

def table_exists(client, dataset_name, table_name):
    try:
        # Use the client to check if the table exists
        table_ref = client.dataset(dataset_name).table(table_name)
        client.get_table(table_ref)  # This will raise an error if the table does not exist
        return True
    except NotFound:
        return False

def create_table(client, dataset_name, table_name, schema):
    """Create a BigQuery table with the specified schema."""
    table_id = f"{client.project}.{dataset_name}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)
    
    try:
        table = client.create_table(table)  # API request
        logging.info(f"Created table {table_id}.")
    except Exception as e:
        logging.error(f"Error creating table {table_id}: {e}")

def load_dataframe_to_bigquery(df, project_id, dataset_name, table_name):
    """Load the DataFrame to BigQuery."""
    if df.empty:
        logging.info("DataFrame is empty; nothing to load into BigQuery.")
        return False

    client = bigquery.Client()
    table_id = f'{project_id}.{dataset_name}.{table_name}'

    # Check and create the table if necessary
    if not table_exists(client, dataset_name, table_name):
        create_table(client, dataset_name, table_name, TABLE_SCHEMA)

    logging.info(f"Loading DataFrame to BigQuery table: {table_id}")

    # Load DataFrame to BigQuery
    try:
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        logging.info(f"Loaded {len(df)} rows into {table_id}.")
        return True
    except Exception as e:
        logging.error(f"Error loading DataFrame to BigQuery: {e}")
        return False
