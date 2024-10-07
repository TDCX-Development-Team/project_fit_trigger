import os
import logging
from google.cloud import storage, bigquery
from convert_to_csv import convert_to_csv

# Initialize GCS and BigQuery clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Configuration constants
PROJECT_ID = 'tdcxai-data-science'  # Define your project ID here
BUCKET_NAME = 'project_fit'
DATASET_NAME = 'project_fit'
TABLE_NAME = 'tbl_alo_roster'

# Set up logging
logging.basicConfig(level=logging.INFO)

def process_file(data, context):
    """Triggered by a change to a Cloud Storage bucket. Convert Excel file to CSV and load to BigQuery."""
    
    file_name = data['name']
    logging.info(f'Processing file: {file_name}')

    try:
        # Download the Excel file from GCS
        temp_file_path = f'/tmp/{file_name}'
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_name)

        # Download the blob to a temporary file
        logging.info(f'Downloading file from bucket: {BUCKET_NAME}/{file_name}...')
        blob.download_to_filename(temp_file_path)

        # Convert Excel to CSV
        csv_file_path = f'/tmp/{file_name}.csv'
        logging.info(f'Converting {file_name} to CSV...')
        convert_to_csv(temp_file_path, csv_file_path)

        # Load the CSV file into BigQuery
        load_csv_to_bigquery(csv_file_path)

        # Optionally, you can delete the temporary files
        os.remove(temp_file_path)
        os.remove(csv_file_path)
        logging.info(f'Successfully processed and cleaned up files for {file_name}.')

    except Exception as e:
        logging.error(f'An error occurred while processing {file_name}: {e}')
        raise  # Rethrow the exception to indicate failure

def load_csv_to_bigquery(csv_file_path):
    """Load the CSV file to BigQuery."""
    table_id = f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}'

    try:
        # Load the CSV file into BigQuery
        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Automatically infer the schema
            source_format=bigquery.SourceFormat.CSV,
        )

        with open(csv_file_path, "rb") as source_file:
            job = bigquery_client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )  # Make an API request

        job.result()  # Wait for the job to complete
        logging.info(f'Loaded {job.output_rows} rows into {table_id}.')

    except Exception as e:
        logging.error(f'An error occurred while loading CSV to BigQuery: {e}')
        raise  # Rethrow the exception to indicate failure
