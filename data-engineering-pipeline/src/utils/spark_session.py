from pyspark.sql import SparkSession
import os

def get_spark_session(app_name="ServiceNow Pipeline"):
    """
    Creates and returns a Spark session with optimized configurations.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Set log level to WARN to reduce console noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark