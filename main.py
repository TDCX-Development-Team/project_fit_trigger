from pyspark.sql import SparkSession
import logging
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from excel_to_pandas import load_excel_to_dataframe, load_dataframe_to_bigquery, create_table, TABLE_SCHEMA  # Ensure you import all necessary functions and variables
from bigquery_upsert import table_exists, read_existing_data, upsert_to_bigquery
from config import PROJECT_ID, DATASET_NAME, TABLE_NAME
import pandas as pd

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
        logging.info(f"Loaded Excel data into DataFrame with {df_new.shape[0]} rows and columns: {df_new.columns.tolist()}")
    except Exception as e:
        logging.error(f"Error loading Excel file to DataFrame from {file_path}: {e}")
        return

    # Ensure df_new is not None or empty
    if df_new is None or df_new.empty:
        logging.error("The new DataFrame is empty or was not loaded correctly.")
        return

    # Initialize BigQuery client
    client = bigquery.Client()

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

    logging.info(f"New DataFrame columns: {df_new.columns.tolist()}")
    logging.info(f"Existing DataFrame columns: {df_existing.columns.tolist()}")

    # Align the columns in both DataFrames for upsert
    missing_in_new = set(df_existing.columns) - set(df_new.columns)
    missing_in_existing = set(df_new.columns) - set(df_existing.columns)

    # Add missing columns to both DataFrames
    for col in missing_in_existing:
        df_new[col] = None  # Add missing columns to new DataFrame
    for col in missing_in_new:
        df_existing[col] = None  # Add missing columns to existing DataFrame

    logging.info(f"Adjusted New DataFrame columns: {df_new.columns.tolist()}")
    logging.info(f"Adjusted Existing DataFrame columns: {df_existing.columns.tolist()}")

    # Perform data loading or upsert operation
    try:
        if df_existing.empty:
            logging.info("No existing data found, performing a full insert.")
            load_successful = load_dataframe_to_bigquery(df_new, PROJECT_ID, DATASET_NAME, TABLE_NAME)  # Load new data
            if not load_successful:
                logging.error("Failed to load new data into BigQuery.")
        else:
            logging.info("Existing data found, performing upsert.")
            # Trigger upsert process
            upsert_successful = upsert_to_bigquery(df_existing, df_new)  # Perform upsert directly
            if not upsert_successful:
                logging.error("Failed to upsert data into BigQuery.")
    except Exception as e:
        logging.error(f"Failed to upsert data into BigQuery: {e}")
        return

    logging.info("Data processing completed successfully.")

