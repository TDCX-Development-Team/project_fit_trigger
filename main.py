from google.cloud import bigquery
import json

def process_file(data, context):
    """Triggered by a change to a Cloud Storage bucket."""
    bucket_name = data['bucket']
    file_name = data['name']
    
    # Define your BigQuery dataset and table name based on the file name
    dataset_id = 'your_dataset_id'
    table_id = f"tbl_{file_name.replace(' ', '_')}"

    # Load data into BigQuery (assuming it's in CSV format)
    client = bigquery.Client()
    uri = f"gs://{bucket_name}/{file_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    load_job = client.load_table_from_uri(uri, f"{dataset_id}.{table_id}", job_config=job_config)
    load_job.result()  # Wait for the job to complete.

    print(f"Loaded {file_name} into {dataset_id}.{table_id}.")
