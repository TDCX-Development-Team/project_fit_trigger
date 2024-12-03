import yaml
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def load_schema(file_path: str) -> dict:
    """Loads schema from a YAML file."""
    try:
        with open(file_path, 'r') as file:
            schema = yaml.safe_load(file)
        return schema['schema']
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise  # Reraise the exception to inform the caller
    except yaml.YAMLError as e:
        logging.error(f"Error reading YAML file: {e}")
        raise  # Reraise the exception to inform the caller
    except KeyError:
        logging.error("YAML file does not contain a 'schema' key.")
        raise  # Reraise the exception to inform the caller

def sync_columns(df: pd.DataFrame, reference_schema: dict) -> pd.DataFrame:
    """Synchronizes DataFrame columns to match a reference schema."""
    if df.empty:
        logging.warning("The input DataFrame is empty. No columns to sync.")
        return df  # Return the empty DataFrame

    if not isinstance(reference_schema, dict):
        logging.error("Reference schema should be a dictionary.")
        raise ValueError("Reference schema should be a dictionary.")

    # Generate column mappings and identify mismatches
    df_columns = {i: col for i, col in enumerate(df.columns)}
    mismatched_columns = {}

    # Compare columns with the reference schema
    for idx, ref_col in reference_schema.items():
        if df_columns.get(idx) != ref_col:
            mismatched_columns[df_columns.get(idx, 'MISSING')] = ref_col
            logging.info(f"Column mismatch at index {idx}: Found '{df_columns.get(idx)}', expected '{ref_col}'")

    # Apply corrections to DataFrame columns
    new_columns = [mismatched_columns.get(col, col) for col in df.columns]
    df.columns = new_columns
    return df
