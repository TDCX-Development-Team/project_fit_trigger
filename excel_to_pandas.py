import pandas as pd

def convert_to_csv(excel_file_path, csv_file_path):
    """Convert an Excel file to CSV, ignoring the first row as header."""
    # Read the Excel file, starting from the second row
    df = pd.read_excel(excel_file_path, header=1)  # Set header=1 to skip the first row

    # Save as CSV
    df.to_csv(csv_file_path, index=False)
    print(f'Converted {excel_file_path} to {csv_file_path}')
