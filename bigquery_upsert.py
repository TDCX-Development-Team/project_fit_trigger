from google.cloud import bigquery
from excel_to_pandas import load_dataframe_to_bigquery
from config import PROJECT_ID, DATASET_NAME, TABLE_NAME
import pandas as pd
import logging
import fsspec
from google.api_core.exceptions import NotFound
from datetime import datetime
import config

# Check if the table exists
def table_exists(client, dataset_name, table_name):
    try:
        # Use the client to check if the table exists
        table_ref = client.dataset(dataset_name).table(table_name)
        client.get_table(table_ref)  # This will raise an error if the table does not exist
        return True
    except NotFound:
        return False

# Read existing data from BigQuery
def read_existing_data(client, dataset_name, table_name):
    query = f"SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}`"
    
    try:
        df = client.query(query).to_dataframe()

        # Print the data types of the existing DataFrame before conversion
        print("Existing DataFrame dtypes before conversion:")
        print(df.dtypes)

        # Debug: print a sample of the 'start_date' column to inspect the values
        if 'start_date' in df.columns:
            print("Existing DataFrame 'start_date' sample before conversion:")
            print(df['start_date'].head())

        # Apply the conversion
        if 'start_date' in df.columns:
            df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.date
        if 'end_date' in df.columns:
            df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date

        # Print data types after conversion
        print("Existing DataFrame dtypes after conversion:")
        print(df.dtypes)

        # Debug: print a sample of the 'start_date' column after conversion
        if 'start_date' in df.columns:
            print("Existing DataFrame 'start_date' sample after conversion:")
            print(df['start_date'].head())

        return df
    except Exception as e:
        logging.error(f"Error reading data from BigQuery: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def upsert_to_bigquery(existing_df, new_df):
    # Convert 'start_date' and 'end_date' in both DataFrames to proper date format
    new_df['start_date'] = pd.to_datetime(new_df['start_date'], errors='coerce').dt.date
    new_df['end_date'] = pd.to_datetime(new_df['end_date'], errors='coerce').dt.date

    existing_df['start_date'] = pd.to_datetime(existing_df['start_date'], errors='coerce').dt.date
    existing_df['end_date'] = pd.to_datetime(existing_df['end_date'], errors='coerce').dt.date

    # Case 1: If the existing_df is empty, just insert new data
    if existing_df.empty:
        logging.info("No existing data found in BigQuery. Inserting new data.")
        load_dataframe_to_bigquery(new_df, PROJECT_ID, DATASET_NAME, TABLE_NAME)
        return

    # Case 2: Merge the new data with existing records
    merged_df = pd.merge(existing_df, new_df, on='EMP_ID', how='outer', suffixes=('_old', '_new'), indicator=True)

    # Define columns to check for changes
    columns_to_check = [
        'Site', 'Name', 'Role', 'Status', 'Leader', 'Manager',
        'Work_Email', 'Wave', 'ALO_Credential_User_Name',  
        'Date_of_Hire', 'Termination_Date', 'Go_Live',  
        'Tenure', 'Contract_Type', 'Contract_End_Date',  
        'Flash_Card_User', 'National_ID', 'Personal_Email',  
        'Birthday', 'Address', 'Barrio_Localidad', 'Phone_Number',  
        'Natterbox'
    ]

    # Adjust for NaN handling and comparison of old vs. new columns
    for column in columns_to_check:
        merged_df[f'{column}_changed'] = merged_df[f'{column}_old'] != merged_df[f'{column}_new']

    # Identify changes within the required columns
    changes_detected = merged_df[[f'{col}_changed' for col in columns_to_check]].any(axis=1)

    # Log and return if no changes are found
    if not changes_detected.any():
        logging.info("No changes detected in records. Skipping upsert.")
        return

    # Separate changed and new records for upsert
    changed_records = merged_df[merged_df['_merge'] == 'both'].copy()
    new_records = merged_df[merged_df['_merge'] == 'right_only'].copy()

    # Update end_date for changed records
    if not changed_records.empty:
        changed_records['end_date'] = datetime.now().date()  # Update end_date to current date
        for col in columns_to_check:
            if (changed_records[f'{col}_old'] != changed_records[f'{col}_new']).any():
                changed_records[col] = changed_records[f'{col}_new']

    # Handle new records
    if not new_records.empty:
        new_records['start_date'] = datetime.now().date()  # Set start_date for new records
        new_records['end_date'] = pd.NaT  # Leave end_date as NaT for new records

    # Combine the changed and new records
    records_to_insert = pd.concat([changed_records, new_records], ignore_index=True)

    # Clean up unnecessary columns
    records_to_insert = records_to_insert.loc[:, ~records_to_insert.columns.str.endswith('_old')]
    records_to_insert = records_to_insert.loc[:, ~records_to_insert.columns.str.endswith('_new')]
    
    if '_merge' in records_to_insert.columns:
        records_to_insert.drop(columns=['_merge'], inplace=True)

    # Log final columns
    logging.info("Final columns to be inserted: %s", records_to_insert.columns.tolist())

    # Load final DataFrame to BigQuery
    if not records_to_insert.empty:
        load_dataframe_to_bigquery(records_to_insert, PROJECT_ID, DATASET_NAME, TABLE_NAME)
    else:
        logging.info("No records to insert into BigQuery.")



