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
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Log initial DataFrame columns and types
    logging.info("Type of existing_df: %s", type(existing_df))
    logging.info("Loaded existing DataFrame: %s", existing_df.head() if isinstance(existing_df, pd.DataFrame) else 'Not a DataFrame')

    logging.info("Type of new_df: %s", type(new_df))
    logging.info("Loaded new DataFrame: %s", new_df.head() if isinstance(new_df, pd.DataFrame) else 'Not a DataFrame')

    # Strip whitespace from column names
    new_df.columns = new_df.columns.str.strip()
    existing_df.columns = existing_df.columns.str.strip()

    # Check for NaN values in 'emp_id' and log warnings
    new_emp_id_nan_count = new_df['emp_id'].isna().sum()
    existing_emp_id_nan_count = existing_df['emp_id'].isna().sum()
    
    if new_emp_id_nan_count > 0:
        logging.warning("New DataFrame contains %d NaN values in 'emp_id'.", new_emp_id_nan_count)
        # Optionally drop rows with NaN 'emp_id' in new_df
        new_df.dropna(subset=['emp_id'], inplace=True)

    if existing_emp_id_nan_count > 0:
        logging.warning("Existing DataFrame contains %d NaN values in 'emp_id'.", existing_emp_id_nan_count)
        # Optionally drop rows with NaN 'emp_id' in existing_df
        existing_df.dropna(subset=['emp_id'], inplace=True)

    # List of date columns to process, excluding 'start_date', 'end_date'
    date_columns = ['date_of_hire', 'termination_date', 'go_live', 'contract_end_date']

    def process_date_columns(df, date_columns):
        for col in date_columns:
            # Replace empty strings and invalid placeholders with NaT
            df[col] = df[col].replace(['', '-'], pd.NaT)
            # Convert to datetime, coercing errors to NaT
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Extract only the date part
            df[col] = df[col].dt.date

            # Log processed date column information
            logging.info("Processed DataFrame '%s' after conversion: %s", col, df[col].dropna().unique())
    
        return df

    # Process date columns for new_df and existing_df
    new_df = process_date_columns(new_df, date_columns)
    existing_df = process_date_columns(existing_df, date_columns)

    # Case 1: If the existing_df is empty, just insert new data
    if existing_df.empty:
        logging.info("No existing data found in BigQuery. Inserting new data.")
        load_dataframe_to_bigquery(new_df, PROJECT_ID, DATASET_NAME, TABLE_NAME)
        return

    # Case 2: Merge the new data with existing records
    merged_df = pd.merge(existing_df, new_df, on='emp_id', how='outer', suffixes=('_old', '_new'), indicator=True)

    # Identify unchanged records
    unchanged_condition = (merged_df['_merge'] == 'both') & \
                          (merged_df.filter(like='_old').fillna('') == merged_df.filter(like='_new').fillna('')).all(axis=1)

    unchanged_records = merged_df[unchanged_condition]

    # If there are identical records, log a message and skip the upsert
    if not unchanged_records.empty:
        logging.info("No changes detected in records with emp_id: %s. Skipping upsert.", unchanged_records['emp_id'].unique())
        return

    # Handle records present in both datasets (changed records)
    changed_records = merged_df[merged_df['_merge'] == 'both'].copy()
    new_records = merged_df[merged_df['_merge'] == 'right_only'].copy()

    # Update end_date for changed records
    if not changed_records.empty:
        for emp_id in changed_records['emp_id'].unique():
            emp_group = changed_records[changed_records['emp_id'] == emp_id]

            # Get the most recent record based on end_date_old
            most_recent_index = emp_group['end_date_old'].idxmax()
            most_recent = changed_records.loc[most_recent_index]

            # Check if new start_date is greater than the most recent end_date
            if pd.isnull(most_recent['end_date_old']) or most_recent['end_date_old'] < most_recent['start_date_new']:
                # Update end_date_old with start_date_new
                changed_records.at[most_recent_index, 'end_date_old'] = most_recent['start_date_new']

            # Update changed fields, excluding emp_id, start_date, and end_date
            for col in existing_df.columns:
                if col not in ['emp_id', 'start_date', 'end_date']:
                    changed_records.at[most_recent_index, col] = most_recent.get(col + '_new', None)

    # Prepare new records
    if not new_records.empty:
        current_date = datetime.now().date()
        # Here you can decide how to handle the new records; if needed, you can set other fields

    # Combine the changed and new records
    records_to_insert = pd.concat([changed_records, new_records], ignore_index=True)

    # Clean up unnecessary columns
    records_to_insert = records_to_insert.loc[:, ~records_to_insert.columns.str.endswith('_old')]
    records_to_insert = records_to_insert.loc[:, ~records_to_insert.columns.str.endswith('_new')]

    if '_merge' in records_to_insert.columns:
        records_to_insert.drop(columns=['_merge'], inplace=True)

    # Explicitly convert date columns to datetime (date only)
    for col in date_columns:
        if col in records_to_insert.columns:
            records_to_insert[col] = pd.to_datetime(records_to_insert[col], errors='coerce').dt.date  # Keep date only

    # Replace empty strings with NaT in final records_to_insert for safety
    for col in date_columns:
        if col in records_to_insert.columns:
            records_to_insert[col] = records_to_insert[col].replace(['', '-'], pd.NaT)  # Clean up empty strings
            logging.info("Final DataFrame '%s' after cleaning: %s", col, records_to_insert[col].dropna().unique())

    # Log final columns
    logging.info("Final columns to be inserted: %s", records_to_insert.columns.tolist())

    # Log the DataFrame before inserting
    logging.info("Records to insert before BigQuery load: %s", records_to_insert)

    # Load final DataFrame to BigQuery only if there are records to insert
    if not records_to_insert.empty:
        logging.info("Inserting records into BigQuery.")
        load_dataframe_to_bigquery(records_to_insert, PROJECT_ID, DATASET_NAME, TABLE_NAME)
    else:
        logging.info("No records to insert into BigQuery.")
