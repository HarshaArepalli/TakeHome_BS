from pyspark.sql import SparkSession
import os

def ingest_raw_data(spark: SparkSession, input_path: str, output_path: str):
    """
    Bronze Layer: Ingest raw CSV data and save as Parquet.
    Preserves schema as-is from source.
    
    Args:
        spark: Active Spark session
        input_path: Path to raw CSV file
        output_path: Path to save bronze layer data
        
    Returns:
        DataFrame: The ingested data
    """
    print(f"[Bronze Layer] Reading raw data from: {input_path}")
    
    # Read raw CSV with schema inference to preserve all data
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
        mode="PERMISSIVE"
    )
    
    record_count = df.count()
    print(f"[Bronze Layer] Records read: {record_count}")
    print(f"[Bronze Layer] Schema:")
    df.printSchema()
    
    # Write to bronze layer as Parquet
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"[Bronze Layer] Data written to: {output_path}")
    
    return df