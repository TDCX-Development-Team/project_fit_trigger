from pyspark.sql import SparkSession
import pandas as pd
from google.cloud import bigquery
import logging

def process_file(event, context):
    logging.basicConfig(level=logging.INFO)
    
    # Initialize Spark session
    try:
        spark = SparkSession.builder \
            .appName("Load Excel to BigQuery") \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0,com.crealytics:spark-excel_2.12:0.13.5") \
            .getOrCreate()
        logging.info("Spark session started successfully.")
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return

    # File path in Google Cloud Storage
    file_path = f"gs://{event['bucket']}/{event['name']}"
    logging.info(f"Reading file from: {file_path}")

    # Read the Excel file using Spark
    try:
        df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("useHeader", "true") \
            .option("inferSchema", "true") \
            .load(file_path)

        logging.info("File read successfully.")
        df.printSchema()
        df.show()
    except Exception as e:
        logging.error(f"Failed to read the Excel file: {e}")
        return

    # BigQuery table details
    project_id = "tdcxai-data-science"  # Replace with your GCP project ID
    dataset_id = "project_fit"  # Replace with your dataset ID
    table_id = "tbl_alo_roster"

    # Write data to BigQuery
    try:
        df.write \
            .format("bigquery") \
            .option("table", f"{project_id}.{dataset_id}.{table_id}") \
            .mode("append") \
            .save()
        logging.info("Data written to BigQuery successfully.")
    except Exception as e:
        logging.error(f"Failed to write data to BigQuery: {e}")
        return

    # Stop the Spark session
    spark.stop()
