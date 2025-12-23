from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg, sum as spark_sum, min as spark_min, max as spark_max

def create_aggregations(spark: SparkSession, input_path: str, output_path: str):
    """
    Gold Layer: Create aggregated views for analytics.
    
    Aggregation: Count tickets by status and priority
    
    Args:
        spark: Active Spark session
        input_path: Path to silver layer data
        output_path: Path to save gold layer data
        
    Returns:
        DataFrame: Aggregated data
    """
    print(f"[Gold Layer] Reading silver data from: {input_path}")
    
    # Read silver layer data
    df = spark.read.parquet(input_path)
    
    print(f"[Gold Layer] Creating aggregations...")
    
    # Aggregation: Count tickets by status and priority
    ticket_summary = df.groupBy("state", "priority") \
        .agg(count("*").alias("ticket_count")) \
        .orderBy("state", "priority")
    
    print(f"[Gold Layer] Aggregation results:")
    ticket_summary.show(100, truncate=False)
    
    # Write to gold layer as CSV for easy consumption
    ticket_summary.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
    
    print(f"[Gold Layer] Aggregated data written to: {output_path}")
    
    return ticket_summary