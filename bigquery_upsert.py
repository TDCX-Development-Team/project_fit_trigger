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
        #print("Existing DataFrame dtypes before conversion:")
        #print(df.dtypes)

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

    try:
        # Strip whitespace from column names
        new_df.columns = new_df.columns.str.strip()
        existing_df.columns = existing_df.columns.str.strip()

        logging.info("Stripped columns in new_df: %s", new_df.columns.tolist())
        logging.info("Stripped columns in existing_df: %s", existing_df.columns.tolist())

        # Define columns to check for presence in DataFrames
        columns_to_check = [
            'emp_id', 'site', 'name', 'role', 'status', 'leader', 'manager',
            'work_email', 'wave', 'alo_credential_user_name',  
            'date_of_hire', 'termination_date', 'go_live',  
            'tenure', 'contract_type', 'contract_end_date',  
            'flash_card_user', 'national_id', 'personal_email',  
            'birthday', 'address', 'barrio_localidad', 'phone_number',  
            'natterbox', 'start_date', 'end_date'
        ]

        # Check for missing columns in new_df and existing_df
        for df_name, df in zip(['new_df', 'existing_df'], [new_df, existing_df]):
            missing_columns = [col for col in columns_to_check if col not in df.columns]
            if missing_columns:
                error_message = f"Columns missing from {df_name}: {', '.join(missing_columns)}"
                logging.error(error_message)
                return {"success": False, "error": error_message}

        # Drop rows with missing emp_id in both DataFrames before merging
        new_df_before_drop = new_df.shape[0]
        existing_df_before_drop = existing_df.shape[0]
        new_df = new_df.dropna(subset=['emp_id'])
        existing_df = existing_df.dropna(subset=['emp_id'])
        logging.info(f"Dropped {new_df_before_drop - new_df.shape[0]} rows from new_df and {existing_df_before_drop - existing_df.shape[0]} rows from existing_df due to missing emp_id.")

        # Convert 'start_date' and 'end_date' in both DataFrames to proper date format
        for df in [new_df, existing_df]:
            try:
                df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.date
                df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date

                if df['start_date'].isnull().any() or df['end_date'].isnull().any():
                    raise ValueError(f"Date conversion failed in one of the rows in DataFrame:\n{df.head()}")
            except Exception as date_conversion_error:
                error_message = f"Error converting date columns: {str(date_conversion_error)}"
                logging.error(error_message)
                return {"success": False, "error": error_message}

        # Ensure 'tenure' and 'birthday' are treated as strings
        new_df['tenure'] = new_df['tenure'].astype(str)
        existing_df['tenure'] = existing_df['tenure'].astype(str)
        new_df['birthday'] = new_df['birthday'].astype(str)
        existing_df['birthday'] = existing_df['birthday'].astype(str)

        # Case 1: If the existing_df is empty, insert new data
        if existing_df.empty:
            try:
                logging.info("No existing data found in BigQuery. Inserting new data.")
                load_dataframe_to_bigquery(new_df, PROJECT_ID, DATASET_NAME, TABLE_NAME)
                return {"success": True, "message": "Inserted new data."}
            except Exception as insert_error:
                error_message = f"Error inserting new data: {str(insert_error)}"
                logging.error(error_message)
                return {"success": False, "error": error_message}

        # Case 2: Process the new data with existing records
        for emp_id in new_df['emp_id'].unique():
            try:
                existing_records = existing_df[existing_df['emp_id'] == emp_id]
                new_records = new_df[new_df['emp_id'] == emp_id]

                if not existing_records.empty and not new_records.empty:
                    # Handle identical records by comparing values (excluding start_date and end_date)
                    try:
                        identical = existing_records.drop(columns=['emp_id', 'start_date', 'end_date']).equals(
                            new_records.drop(columns=['emp_id', 'start_date', 'end_date'])
                        )

                        if identical:
                            logging.info(f"Identical records found for emp_id {emp_id}. Ignoring these records.")
                            continue  # Skip processing for identical records
                    except Exception as e:
                        logging.error(f"Error comparing records for emp_id {emp_id}: {e}")
                        return {"success": False, "error": str(e)}

                    # Update end_date for the most recent existing record
                    last_index = existing_records.index[-1]
                    existing_df.at[last_index, 'end_date'] = datetime.now().date()
                    logging.info(f"Updated end_date for emp_id {emp_id} to current date.")

                # Append new records
                for new_record in new_records.itertuples(index=False):
                    new_record_df = pd.DataFrame([new_record._asdict()])
                    new_record_df['start_date'] = datetime.now().date()
                    new_record_df['end_date'] = pd.NaT  # Set end_date as NaT for new records
                    existing_df = pd.concat([existing_df, new_record_df], ignore_index=True)
                    logging.info(f"Appended new record for emp_id {emp_id}. Current size of existing_df: {existing_df.shape[0]}.")

            except Exception as processing_error:
                error_message = f"Error processing emp_id {emp_id}: {str(processing_error)}"
                logging.error(error_message)
                return {"success": False, "error": error_message}

        # Final DataFrame preparation and insertion
        try:
            existing_df.columns = existing_df.columns.str.replace('_old', '', regex=False).str.replace('_new', '', regex=False)
            records_to_insert = existing_df[columns_to_check]
            records_to_insert.reset_index(drop=True, inplace=True)

            if records_to_insert.empty:
                return {"success": True, "message": "No records to insert into BigQuery."}

            logging.info(f"Inserting {len(records_to_insert)} records into BigQuery.")
            load_dataframe_to_bigquery(records_to_insert, PROJECT_ID, DATASET_NAME, TABLE_NAME)
            return {"success": True, "message": f"Inserted {len(records_to_insert)} records into BigQuery."}

        except Exception as final_insertion_error:
            error_message = f"Error during final insertion: {str(final_insertion_error)}"
            logging.error(error_message)
            return {"success": False, "error": error_message}

    except Exception as general_error:
        error_message = f"General error in upsert_to_bigquery: {str(general_error)}"
        logging.error(error_message)
        return {"success": False, "error": error_message}








# def upsert_to_bigquery(existing_df, new_df):
#     # Set up logging
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
#     # Log initial DataFrame columns
#     #logging.info("Loaded existing DataFrame: %s", existing_df.head())
#     #logging.info("Loaded new DataFrame: %s", new_df.head())
    
#     # Strip whitespace from column names
#     new_df.columns = new_df.columns.str.strip()
#     existing_df.columns = existing_df.columns.str.strip()
    
#     # Convert 'start_date' and 'end_date' in both DataFrames to proper date format
#     new_df['start_date'] = pd.to_datetime(new_df['start_date'], errors='coerce').dt.date
#     new_df['end_date'] = pd.to_datetime(new_df['end_date'], errors='coerce').dt.date
#     existing_df['start_date'] = pd.to_datetime(existing_df['start_date'], errors='coerce').dt.date
#     existing_df['end_date'] = pd.to_datetime(existing_df['end_date'], errors='coerce').dt.date

#     # Define columns to check for presence in DataFrames
#     columns_to_check = [
#         'emp_id', 'site', 'name', 'role', 'status', 'leader', 'manager',
#         'work_email', 'wave', 'alo_credential_user_name',  
#         'date_of_hire', 'termination_date', 'go_live',  
#         'tenure', 'contract_type', 'contract_end_date',  
#         'flash_card_user', 'national_id', 'personal_email',  
#         'birthday', 'address', 'barrio_localidad', 'phone_number',  
#         'natterbox', 'start_date', 'end_date'
#     ]
    
#     # Check for missing columns in the new DataFrame
#     for col in columns_to_check:
#         if col not in new_df.columns:
#             logging.error(f"Column '{col}' is missing from the new DataFrame.")
#             return  # Exit the function if a column is missing

#     # Check for missing columns in the existing DataFrame
#     for col in columns_to_check:
#         if col not in existing_df.columns:
#             logging.error(f"Column '{col}' is missing from the existing DataFrame.")
#             return  # Exit the function if a column is missing

#     # Case 1: If the existing_df is empty, just insert new data
#     if existing_df.empty:
#         logging.info("No existing data found in BigQuery. Inserting new data.")
#         load_dataframe_to_bigquery(new_df, PROJECT_ID, DATASET_NAME, TABLE_NAME)
#         return

#     # Case 2: Merge the new data with existing records
#     merged_df = pd.merge(existing_df, new_df, on='emp_id', how='outer', suffixes=('_old', '_new'), indicator=True)

#     # Identify unchanged records
#     identical_records = merged_df[(
#         merged_df['_merge'] == 'both') & 
#         (merged_df[columns_to_check].isnull().all(axis=1) | 
#          (merged_df[[col + '_old' for col in columns_to_check]].fillna('') == 
#           merged_df[[col + '_new' for col in columns_to_check]].fillna('')).all(axis=1))
#     ]

#     # If there are identical records, log a message and skip the upsert
#     if not identical_records.empty:
#         logging.info("No changes detected in records with emp_id: %s. Skipping upsert.", identical_records['emp_id'].unique())
#         return

#     # Handle records present in both datasets
#     changed_records = merged_df[merged_df['_merge'] == 'both'].copy()
#     new_records = merged_df[merged_df['_merge'] == 'right_only'].copy()

#     # Update end_date for changed records
#     if not changed_records.empty:
#         # Iterate through changed records to update end_date
#         for emp_id in changed_records['emp_id'].unique():
#             emp_group = changed_records[changed_records['emp_id'] == emp_id]
            
#             # Get the most recent record based on end_date_old
#             most_recent = emp_group.loc[emp_group['end_date_old'].idxmax()]
            
#             # Check if new start_date is greater than the current most recent end_date
#             if most_recent['end_date_old'] < most_recent['start_date_new']:
#                 # Update end_date of the most recent record
#                 most_recent['end_date_old'] = most_recent['start_date_new']

#             # Update only the changed fields, excluding emp_id, start_date, and end_date
#             for col in columns_to_check:
#                 if col not in ['emp_id', 'start_date', 'end_date']:  # Exclude emp_id, start_date, end_date
#                     most_recent[col] = most_recent[col + '_new']

#     # Prepare new records
#     if not new_records.empty:
#         new_records['start_date'] = datetime.now().date()  # Set start_date for new records
#         new_records['end_date'] = datetime.now().date()    # Set end_date for new records

#     # Combine the changed and new records
#     records_to_insert = pd.concat([changed_records, new_records], ignore_index=True)

#     # Clean up unnecessary columns
#     records_to_insert = records_to_insert.loc[:, ~records_to_insert.columns.str.endswith('_old')]
#     records_to_insert = records_to_insert.loc[:, ~records_to_insert.columns.str.endswith('_new')]

#     if '_merge' in records_to_insert.columns:
#         records_to_insert.drop(columns=['_merge'], inplace=True)

#     # Ensure only the original columns are in the final DataFrame
#     records_to_insert = records_to_insert[columns_to_check]

#     # Log final columns
#     logging.info("Final columns to be inserted: %s", records_to_insert.columns.tolist())

#     # Load final DataFrame to BigQuery only if there are records to insert
#     if not records_to_insert.empty:
#         logging.info("Inserting records into BigQuery.")
#         load_dataframe_to_bigquery(records_to_insert, PROJECT_ID, DATASET_NAME, TABLE_NAME)
#     else:
#         logging.info("No records to insert into BigQuery.")