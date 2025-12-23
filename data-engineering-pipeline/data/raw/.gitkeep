# bronze/bronze_layer.py

from pyspark.sql import SparkSession

def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Bronze Layer") \
        .getOrCreate()

    # Read the raw CSV file into a DataFrame
    df = spark.read.csv("path/to/servicenow_incidents_10k.csv", header=True, inferSchema=True)

    # Write the DataFrame to a storage location in raw format
    df.write.mode("overwrite").parquet("path/to/bronze_layer_output")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()