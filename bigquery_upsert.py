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
# def read_existing_data(client, dataset_name, table_name):
#     query = f"SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}`"
    
#     try:
#         df = client.query(query).to_dataframe()
        
#         # Log the number of rows retrieved
#         logging.info(f"Query result for {dataset_name}.{table_name} has {df.shape[0]} rows.")
        
#         if 'start_date' in df.columns:
#             logging.info(df['start_date'].head())

#         # Apply the conversion
#         if 'start_date' in df.columns:
#             df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.date
#         if 'end_date' in df.columns:
#             df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date

#         if 'start_date' in df.columns:
#             logging.info(df['start_date'].head())

#         return df
#     except Exception as e:
#         logging.error(f"Error reading data from BigQuery: {e}")
#         return pd.DataFrame()  # Return empty DataFrame on error


def read_existing_data(client, dataset_name, table_name):
    query = f"""
    SELECT * EXCEPT(latest_record) 
    FROM (
      SELECT *, 
             MAX(start_date) OVER (PARTITION BY name ORDER BY start_date) = start_date AS latest_record
      FROM `{PROJECT_ID}.{dataset_name}.{table_name}`
      ORDER BY name, start_date
    ) 
    WHERE latest_record = true
    """
    
    try:
        # Execute query and load results into a DataFrame
        df = client.query(query).to_dataframe()
        
        # Log the number of rows retrieved
        logging.info(f"Query result for {dataset_name}.{table_name} has {df.shape[0]} rows.")
        
        # Preview start_date column before conversion
        if 'start_date' in df.columns:
            logging.info("Preview of start_date column before conversion:")
            logging.info(df['start_date'].head())
        
        # Convert date columns to ensure consistent format
        if 'start_date' in df.columns:
            df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.date
        if 'end_date' in df.columns:
            df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date
        
        # Log start_date after conversion
        if 'start_date' in df.columns:
            logging.info("Preview of start_date column after conversion:")
            logging.info(df['start_date'].head())
        
        return df
    
    except Exception as e:
        # Log the error and return an empty DataFrame
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

        records_to_insert = []

        # Case 2: Process the new data with existing records
        for emp_id in new_df['emp_id'].unique():
            try:
                existing_records = existing_df[existing_df['emp_id'] == emp_id]
                new_records = new_df[new_df['emp_id'] == emp_id]

                if not existing_records.empty and not new_records.empty:
                    # Compare all relevant columns (excluding emp_id, start_date, and end_date)
                    comparison_columns = [col for col in columns_to_check if col not in ['emp_id', 'start_date', 'end_date']]
                    existing_values = existing_records[comparison_columns].values[0]
                    new_values = new_records[comparison_columns].values[0]

                    # Convert to strings for comparison
                    existing_values = existing_values.astype(str)
                    new_values = new_values.astype(str)

                    changes_detected = not (existing_values == new_values).all()

                    if changes_detected:
                        # Check for specific columns that have changed
                        changed_columns = [col for col, old_val, new_val in zip(comparison_columns, existing_values, new_values) if old_val != new_val]
                        
                        # Log the changes detected
                        if changed_columns:
                            logging.info(f"Changes detected for emp_id {emp_id}: {', '.join(changed_columns)}.")

                        # Update end_date for the most recent existing record
                        last_index = existing_records.index[-1]
                        existing_df.at[last_index, 'end_date'] = datetime.now().date()
                        logging.info(f"Updated end_date for emp_id {emp_id} to current date.")

                        # Append the updated existing record
                        updated_existing_record_df = existing_df.loc[[last_index]]  # Get the updated record
                        records_to_insert.append(updated_existing_record_df)

                        # Append the new record only if it does not match the existing record
                        new_record_df = pd.DataFrame([new_records.iloc[0].to_dict()])
                        new_record_df['start_date'] = datetime.now().date()
                        new_record_df['end_date'] = datetime.now().date()
                        records_to_insert.append(new_record_df)

                    else:
                        # Log that the record is identical
                        logging.info(f"No changes detected for emp_id {emp_id}. Skipping insertion.")

                # If emp_id is new, simply insert it
                if emp_id not in existing_df['emp_id'].values:
                    for new_record in new_records.itertuples(index=False):
                        new_record_df = pd.DataFrame([new_record._asdict()])
                        new_record_df['start_date'] = datetime.now().date()
                        new_record_df['end_date'] = datetime.now().date()
                        records_to_insert.append(new_record_df)
                        logging.info(f"Inserted new emp_id {emp_id} with record: {new_record_df.to_dict(orient='records')}.")

            except Exception as processing_error:
                error_message = f"Error processing emp_id {emp_id}: {str(processing_error)}"
                logging.error(error_message)
                return {"success": False, "error": error_message}

        # Final DataFrame preparation and insertion
        try:
            if records_to_insert:
                records_to_insert_df = pd.concat(records_to_insert, ignore_index=True)
                records_to_insert_df.columns = records_to_insert_df.columns.str.replace('_old', '', regex=False).str.replace('_new', '', regex=False)
                records_to_insert_df = records_to_insert_df[columns_to_check]  # Ensure only relevant columns are included
                records_to_insert_df.reset_index(drop=True, inplace=True)

                logging.info(f"Inserting {len(records_to_insert_df)} records into BigQuery.")
                load_dataframe_to_bigquery(records_to_insert_df, PROJECT_ID, DATASET_NAME, TABLE_NAME)
                return {"success": True, "message": f"Inserted {len(records_to_insert_df)} records into BigQuery."}
            else:
                return {"success": True, "message": "No records to insert into BigQuery."}

        except Exception as final_insertion_error:
            error_message = f"Error during final insertion: {str(final_insertion_error)}"
            logging.error(error_message)
            return {"success": False, "error": error_message}

    except Exception as general_error:
        error_message = f"General error in upsert_to_bigquery: {str(general_error)}"
        logging.error(error_message)
        return {"success": False, "error": error_message}
