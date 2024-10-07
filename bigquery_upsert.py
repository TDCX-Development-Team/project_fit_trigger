from google.cloud import bigquery
from excel_to_pandas import load_excel_to_dataframe
from config import PROJECT_ID, DATASET_NAME, TABLE_NAME
import pandas as pd

# Check if the table exists
def table_exists():
    client = bigquery.Client()
    dataset_ref = client.dataset(DATASET_NAME)
    table_ref = dataset_ref.table(TABLE_NAME)
    
    try:
        client.get_table(table_ref)  # Make an API call to check if the table exists
        print(f"Table {TABLE_NAME} exists.")
        return True
    except bigquery.NotFound:
        print(f"Table {TABLE_NAME} does not exist.")
        return False

# Create BigQuery table dynamically based on the DataFrame schema
def create_table(df):
    client = bigquery.Client()

    # Dynamically create schema from the DataFrame
    schema = []
    for col, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            schema.append(bigquery.SchemaField(col, "INTEGER"))
        elif "float" in str(dtype):
            schema.append(bigquery.SchemaField(col, "FLOAT"))
        elif "datetime" in str(dtype):
            schema.append(bigquery.SchemaField(col, "DATE"))
        else:
            schema.append(bigquery.SchemaField(col, "STRING"))

    table_ref = bigquery.Table(f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}', schema=schema)
    table = client.create_table(table_ref)  # Create the table
    print(f"Created table {TABLE_NAME} with schema based on DataFrame columns.")

# Read existing data from BigQuery
def read_existing_data():
    client = bigquery.Client()
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`"
    df = client.query(query).to_dataframe()
    return df

# Upsert data into BigQuery
def upsert_to_bigquery(existing_df, new_df):
    # Perform an outer join on EMP ID
    merged_df = pd.merge(existing_df, new_df, on='EMP_ID', how='outer', suffixes=('_old', '_new'), indicator=True)

    # Handle any changes or new records dynamically for all columns
    updated_records = merged_df[merged_df['_merge'] == 'both'].copy()
    new_records = merged_df[merged_df['_merge'] == 'right_only'].copy()
    
    # Compare each column dynamically for changes
    changed_records = pd.DataFrame()
    for column in existing_df.columns:
        if column in new_df.columns:  # Compare columns that exist in both
            changes = merged_df[merged_df[f'{column}_old'] != merged_df[f'{column}_new']]
            changed_records = pd.concat([changed_records, changes])

    # Set end_date for old records that changed
    existing_df.loc[existing_df['EMP_ID'].isin(changed_records['EMP_ID']), 'end_date'] = pd.Timestamp.today().date()
    
    # Add start_date to new records
    new_records['start_date'] = pd.Timestamp.today().date()
    new_records['end_date'] = pd.NaT  # Set end_date to null for new records

    # Combine updated and new records
    records_to_insert = pd.concat([changed_records, new_records])

    # Load the combined data into BigQuery
    load_dataframe_to_bigquery(records_to_insert)
