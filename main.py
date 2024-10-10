from pyspark.sql import SparkSession
import logging
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from excel_to_pandas import load_excel_to_dataframe, load_dataframe_to_bigquery, create_table, TABLE_SCHEMA  # Ensure you import all necessary functions and variables
from bigquery_upsert import table_exists, read_existing_data, upsert_to_bigquery
from config import PROJECT_ID, DATASET_NAME, TABLE_NAME


def process_file(event, context):
    logging.basicConfig(level=logging.INFO)
    
    # Get the bucket and file name
    bucket_name = event['bucket']
    file_name = event['name']
    logging.info(f"Processing file {file_name} from bucket {bucket_name}")

    # File path in Google Cloud Storage
    file_path = f"gs://{bucket_name}/{file_name}"

    # Read the file as a Pandas DataFrame
    try:
        df_new = load_excel_to_dataframe(file_path)
        logging.info(f"Loaded Excel data into DataFrame with {df_new.shape[0]} rows and columns: {df_new.columns.tolist()}")
    except Exception as e:
        logging.error(f"Error loading Excel file to DataFrame from {file_path}: {e}")
        return

    # Initialize BigQuery client
    client = bigquery.Client()

    # Check if BigQuery table exists
    if not table_exists(client, DATASET_NAME, TABLE_NAME):
        logging.info("Table doesn't exist. Creating the table...")
        create_table(client, DATASET_NAME, TABLE_NAME, TABLE_SCHEMA)  # Call create_table from excel_to_pandas

    # Read existing data from BigQuery
    try:
        # Include necessary arguments
        df_existing = read_existing_data(client, DATASET_NAME, TABLE_NAME)
        logging.info(f"Loaded existing data from BigQuery with {df_existing.shape[0]} rows.")
    except Exception as e:
        logging.error(f"Error reading existing data from BigQuery: {e}")
        return

    # Perform upsert operation
    try:
        upsert_to_bigquery(df_existing, df_new)
        logging.info("Upsert to BigQuery completed successfully.")
    except Exception as e:
        logging.error(f"Failed to upsert data into BigQuery: {e}")
