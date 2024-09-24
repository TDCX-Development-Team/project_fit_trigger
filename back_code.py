import json
from google.cloud import bigquery
from google.cloud import storage

def gcs_to_bigquery(data, context):
    """Triggered by a change to a Cloud Storage bucket."""
    
    # Get the file name from the event
    file_name = data['name']
    bucket_name = data['bucket']
    
    # Create BigQuery client
    bq_client = bigquery.Client()
    
    # Set your dataset and table names
    dataset_id = 'project_fit'  # Update this
    table_id = f'tbl_{file_name.replace(" ", "_")}'  # Replace spaces with underscores
    
    # Load data to BigQuery
    uri = f'gs://{bucket_name}/{file_name}'
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,  # Change this based on your file format
        autodetect=True,
    )

    load_job = bq_client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )
    
    load_job.result()  # Wait for the job to complete
    print(f'Loaded {file_name} into {table_id}.')