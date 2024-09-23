from pyspark.sql import SparkSession

def process_file(event, context):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Load Excel to BigQuery") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0,com.crealytics:spark-excel_2.12:0.13.5") \
        .getOrCreate()

    # File path in Google Cloud Storage
    file_path = f"gs://{event['bucket']}/{event['name']}"

    # Read the Excel file using Spark
    df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("useHeader", "true") \
        .option("inferSchema", "true") \
        .load(file_path)

    # Print the inferred schema
    df.printSchema()

    # Show a preview of the data
    df.show()

    # BigQuery table details
    project_id = "tdcxai-data-science"  # Replace with your GCP project ID
    dataset_id = "project_fit"  # Replace with your dataset ID
    table_id = "tbl_alo_roster"

    # Write data to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{table_id}") \
        .mode("append") \
        .save()

    # Stop the Spark session
    spark.stop()
