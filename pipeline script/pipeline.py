from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, lag
from pyspark.sql.window import Window
import logging
import shutil
import os

# Set up logging for monitoring
logging.basicConfig(filename='pipeline.com', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Monitoring function to log metrics
def log_metric(metric_name, value):
    logging.info(f"{metric_name}: {value}")

# Create SparkSession with additional memory allocation and settings for better performance
spark = SparkSession.builder \
    .appName("PipelineDataPipeline") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.default.parallelism", "100") \
    .getOrCreate()

# Set logging level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Set a stable directory for temporary Spark files
spark.conf.set("spark.local.dir", "C:/Users/vijay/spark_temp_dir")

### Step 1: Ingestion ###
def ingest_data(sources):
    """
    This step handles data ingestion from multiple sources, such as Pipeline CSV files. The function reads the files, renames columns for consistency, and merges them into a unified dataframe.
    """
    try:
        data_frames = []
        standardized_columns = []

        # Load dataframes and collect all unique columns
        for source in sources:
            if source['type'] == 'csv':
                df = spark.read.csv(source['location'], header=True, inferSchema=True)

                # Ensure Date column is consistently named
                if "Date" in df.columns:
                    df = df.withColumnRenamed("Date", "Date")

                # Rename `Close` column to include the source identifier (e.g., Close_<source_name>)
                close_column_count = sum(1 for col_name in df.columns if col_name == "Close")
                if close_column_count > 0:
                    for i in range(close_column_count):
                        df = df.withColumnRenamed("Close", f"Close_{source['name']}_{i + 1}")

                df = df.withColumn('Source', lit(source['name']))
                data_frames.append(df)

                # Collect columns for standardization
                standardized_columns.extend(df.columns)

                log_metric(f"Data Ingestion from {source['name']}", "Success")
                log_metric(f"{source['name']} Data Volume", df.count())

        # Standardize columns for all dataframes to match
        standardized_columns = list(set(standardized_columns))  # Unique columns

        standardized_data_frames = []
        for df in data_frames:
            # Add missing columns as NULL
            for col_name in standardized_columns:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None))
            # Reorder columns to match the full set of columns
            df = df.select(*standardized_columns)
            standardized_data_frames.append(df)

        # Combine all the dataframes using union
        combined_data = standardized_data_frames[0]
        for df in standardized_data_frames[1:]:
            combined_data = combined_data.union(df)

        return combined_data

    except Exception as e:
        log_metric("Data Ingestion Error", str(e))
        raise

### Step 2: Transformation and Processing ###
def transform_data(data):
    """
    This step focuses on minimal data cleaning since the data from Yahoo Finance is generally complete, consistent, and standardized. The transformation step includes checks for missing values, consistent column naming, and correct data types, ensuring the data is ready for downstream calculations.
    Operations: data standardization, aggregation, filtering, deduplication.
    """
    try:
        # Data Standardization: Ensure Date is in the correct format
        data = data.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

        # Filter out rows with null Date or null in any "Close" columns
        close_columns = [col_name for col_name in data.columns if col_name.startswith("Close_")]
        data = data.filter(col("Date").isNotNull())
        for col_name in close_columns:
            data = data.filter(col(col_name).isNotNull())

        # Feature Engineering: Calculate daily returns for each "Close_<source>" column
        for col_name in close_columns:
            asset_name = col_name.split("_")[1]  # Extract the asset name
            window_spec = Window.partitionBy("Source").orderBy("Date")
            data = data.withColumn(f"Previous_{col_name}", lag(col_name).over(window_spec))
            data = data.withColumn(f"Daily_Return_{asset_name}",
                                   (col(col_name) - col(f"Previous_{col_name}")) / col(f"Previous_{col_name}"))

            # Drop the "Previous_<col_name>" as it's only a helper column
            data = data.drop(f"Previous_{col_name}")

        log_metric("Data Transformation", "Success")
        log_metric("Transformed Data Volume", data.count())

        return data

    except Exception as e:
        log_metric("Data Transformation Error", str(e))
        raise

### Step 3: Load and Data Sharing ###
def load_data(data, storage_path="processed_data"):
    """
    Load the processed data to a storage system.
    In this example, save it as CSV files.
    """
    try:
        # Write the data to CSV with a consistent schema to avoid renaming issues
        data.write.option("header", "true").csv(storage_path, mode="overwrite")
        log_metric("Data Load", "Success")
        log_metric("Final Stored Data Volume", data.count())
        print(f"Data pipeline completed. Data saved to {storage_path}")

    except Exception as e:
        log_metric("Data Load Error", str(e))
        raise

### Main Pipeline Function ###
def data_pipeline():
    """
    Full pipeline from ingestion to loading.
    Includes monitoring for issues and logs metrics.
    """
    try:
        # Step 1: Ingest Data
        sources = [
            {"name": "historical_data", "type": "csv", "location": "historical_price_data.csv"},
            {"name": "realtime_data", "type": "csv", "location": "realtime_price_data.csv"}
            # Additional data sources can be added here
        ]
        ingested_data = ingest_data(sources)

        # Step 2: Transform and Process Data
        transformed_data = transform_data(ingested_data)

        # Step 3: Load and Data Sharing
        load_data(transformed_data)

    except Exception as e:
        log_metric("Pipeline Failure", str(e))
        print(f"Data pipeline failed. Check the logs for details. Error: {e}")

    finally:
        # Ensure Spark session is stopped to release resources
        spark.stop()
        # Clean up temporary directories if they still exist
        temp_dir = "C:/Users/vijay/spark_temp_dir"
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                log_metric("Temporary Directory Cleanup", "Success")
            except FileNotFoundError:
                log_metric("Temporary Directory Cleanup", "Directory already removed by Spark")
            except Exception as e:
                log_metric("Temporary Directory Cleanup Error", str(e))

# Run the data pipeline
data_pipeline()
