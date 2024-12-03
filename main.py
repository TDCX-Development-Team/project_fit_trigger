from pyspark.sql import SparkSession
import logging
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from excel_to_pandas import load_excel_to_dataframe, load_dataframe_to_bigquery, create_table, TABLE_SCHEMA  # Ensure you import all necessary functions and variables
from bigquery_upsert import table_exists, read_existing_data, upsert_to_bigquery
from config import PROJECT_ID, DATASET_NAME, TABLE_NAME
import pandas as pd
import time
from google.cloud import bigquery_datatransfer_v1

def process_file(event, context):
    logging.basicConfig(level=logging.INFO)

    # Get the bucket and file name
    bucket_name = event['bucket']
    file_name = event['name']
    logging.info(f"Processing file {file_name} from bucket {bucket_name}")

    # File path in Google Cloud Storage
    file_path = f"gs://{bucket_name}/{file_name}"

    # Load Excel data into a Pandas DataFrame
    df_new = None
    try:
        df_new = load_excel_to_dataframe(file_path)
        logging.info(f"Loaded Excel data into DataFrame with {df_new.shape[0]} rows.")
    except Exception as e:
        logging.error(f"Error loading Excel file to DataFrame from {file_path}: {e}")
        return

    # Ensure df_new is not None or empty
    if df_new is None or df_new.empty:
        logging.error("The new DataFrame is empty or was not loaded correctly.")
        return

    # Drop rows with missing emp_id
    if 'emp_id' not in df_new.columns:
        logging.error("New DataFrame is missing 'emp_id' column.")
        return
    if df_new['emp_id'].isnull().any():
        logging.warning("New DataFrame contains rows with missing 'emp_id'. Dropping those rows.")
        df_new.dropna(subset=['emp_id'], inplace=True)

    # Initialize BigQuery client
    try:
        client = bigquery.Client()
    except Exception as e:
        logging.error(f"Error initializing BigQuery client: {e}")
        return

    # Check if the BigQuery table exists
    if not table_exists(client, DATASET_NAME, TABLE_NAME):
        logging.info("BigQuery table does not exist. Creating the table...")
        try:
            create_table(client, DATASET_NAME, TABLE_NAME, TABLE_SCHEMA)
            logging.info("Table created successfully.")
        except Exception as e:
            logging.error(f"Failed to create table: {e}")
            return

    # Read existing data from BigQuery
    try:
        df_existing = read_existing_data(client, DATASET_NAME, TABLE_NAME)
        logging.info(f"Loaded existing data from BigQuery with {df_existing.shape[0]} rows.")
    except NotFound:
        logging.info("Table exists but no data found. Proceeding with full insert.")
        df_existing = pd.DataFrame()  # No existing data
    except Exception as e:
        logging.error(f"Error reading existing data from BigQuery: {e}")
        return

    # Drop rows with missing emp_id in existing data
    if not df_existing.empty:
        if 'emp_id' not in df_existing.columns:
            logging.error("Existing DataFrame is missing 'emp_id' column.")
            return
        if df_existing['emp_id'].isnull().any():
            logging.warning("Existing DataFrame contains rows with missing 'emp_id'. Dropping those rows.")
            df_existing.dropna(subset=['emp_id'], inplace=True)

    # Log DataFrame types and shapes
    logging.info(f"New DataFrame columns: {df_new.columns.tolist()}")
    logging.info(f"Existing DataFrame columns: {df_existing.columns.tolist()}")
    logging.info(f"Shape of New DataFrame: {df_new.shape}")
    logging.info(f"Shape of Existing DataFrame: {df_existing.shape}")

    # Define columns to check
    columns_to_check = [
        'emp_id', 'site', 'name', 'role', 'status', 'leader', 'manager', 
        'work_email', 'wave', 'alo_credential_user_name', 'date_of_hire', 
        'termination_date', 'go_live', 'tenure', 'contract_type', 
        'contract_end_date', 'flash_card_user', 'national_id', 
        'personal_email', 'birthday', 'address', 'barrio_localidad', 
        'phone_number', 'natterbox', 'start_date', 'end_date'
    ]

    # Align existing DataFrame types to match new DataFrame
    for column in df_new.columns:
        if column in df_existing.columns:
            df_existing[column] = df_existing[column].astype(df_new[column].dtype)
        else:
            df_existing[column] = pd.Series([None] * len(df_existing), dtype=df_new[column].dtype)

    # Ensure both DataFrames have matching columns
    missing_in_new = set(df_existing.columns) - set(df_new.columns)
    missing_in_existing = set(df_new.columns) - set(df_existing.columns)

    # Log missing columns
    if missing_in_new:
        logging.warning(f"Missing columns in new DataFrame: {missing_in_new}")
    if missing_in_existing:
        logging.warning(f"Missing columns in existing DataFrame: {missing_in_existing}")

    for col in missing_in_existing:
        df_new[col] = None
    for col in missing_in_new:
        df_existing[col] = None

    # Check data types consistency after alignment
    if not all(df_new.dtypes == df_existing.dtypes):
        logging.error("Data type mismatch between new and existing DataFrames after alignment.")
        return

    # Check for changes before performing upsert
    if not df_existing.empty:
        # Compare DataFrames to see if they are the same
        if df_existing.equals(df_new):
            logging.info("No changes detected in new data. Skipping upsert.")
            return  # Exit if there are no changes

    # Perform data loading or upsert operation
    try:
        if df_existing.empty:
            logging.info("No existing data found, performing a full insert.")
            load_successful = load_dataframe_to_bigquery(df_new, PROJECT_ID, DATASET_NAME, TABLE_NAME)
            if not load_successful:
                logging.error("Failed to load new data into BigQuery.")
        else:
            logging.info("Existing data found, performing upsert.")
            df_existing = df_existing.reindex(columns=columns_to_check)
            df_new = df_new.reindex(columns=columns_to_check)

            upsert_result = upsert_to_bigquery(df_existing, df_new)
            if not upsert_result['success']:
                logging.error(f"Failed to upsert data into BigQuery: {upsert_result['error']}")
    except Exception as e:
        logging.error(f"Failed to upsert data into BigQuery: {e}")
        return

    logging.info("Data processing completed successfully.")
<<<<<<< HEAD
    # TRIGGER SCHEDULED QUERY
=======
     # TRIGGER SCHEDULED QUERY
>>>>>>> faee8e9593257ea17b0f6b0a4de31c47017124eb
    transferid = '671d5600-0000-2ecb-91d3-089e0831d8c8'
    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    projectid = 'tdcxai-data-science'
    parent = client.project_transfer_config_path(projectid, transferid)
    start_time = bigquery_datatransfer_v1.types.Timestamp(seconds=int(time.time() + 10))
    response = client.start_manual_transfer_runs(parent, requested_run_time=start_time)
    print('Scheduled Query Triggered')

