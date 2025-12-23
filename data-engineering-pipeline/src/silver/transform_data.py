from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, to_timestamp, 
    when, upper, concat_ws, coalesce
)

def normalize_column_names(df):
    """
    Normalize column names to snake_case.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with normalized column names
    """
    for column in df.columns:
        # Replace spaces and special chars with underscore, convert to lowercase
        new_name = column.strip().lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(column, new_name)
    return df

def clean_and_transform(spark: SparkSession, input_path: str, output_path: str):
    """
    Silver Layer: Clean and transform bronze data.
    
    - Normalize column names
    - Remove invalid records (missing number or sys_id)
    - Convert date fields to proper timestamp format
    - Standardize text fields
    
    Args:
        spark: Active Spark session
        input_path: Path to bronze layer data
        output_path: Path to save silver layer data
        
    Returns:
        DataFrame: Cleaned and transformed data
    """
    print(f"[Silver Layer] Reading bronze data from: {input_path}")
    
    # Read bronze layer data
    df = spark.read.parquet(input_path)
    
    initial_count = df.count()
    print(f"[Silver Layer] Initial record count: {initial_count}")
    
    # Step 1: Normalize column names
    df = normalize_column_names(df)
    
    # Step 2: Remove invalid records (missing ticket number or sys_id)
    df = df.filter(
        (col("number").isNotNull()) & 
        (col("sys_id").isNotNull()) &
        (trim(col("number")) != "") &
        (trim(col("sys_id")) != "")
    )
    
    # Step 3: Convert date fields to proper timestamp format
    # Assuming dates are in format: M/d/yyyy H:mm
    df = df.withColumn("opened_at", to_timestamp(col("opened_at"), "M/d/yyyy H:mm"))
    df = df.withColumn("closed_at", to_timestamp(col("closed_at"), "M/d/yyyy H:mm"))
    
    # Step 4: Standardize text fields
    # Trim whitespace for key fields
    text_columns = ["state", "priority", "category", "subcategory", "assignment_group"]
    for column in text_columns:
        if column in df.columns:
            df = df.withColumn(column, trim(col(column)))
    
    # Convert boolean strings to actual booleans
    if "sla_breached" in df.columns:
        df = df.withColumn(
            "sla_breached",
            when(upper(col("sla_breached")) == "TRUE", True)
            .when(upper(col("sla_breached")) == "FALSE", False)
            .otherwise(None)
        )
    
    if "knowledge_linked" in df.columns:
        df = df.withColumn(
            "knowledge_linked",
            when(upper(col("knowledge_linked")) == "TRUE", True)
            .when(upper(col("knowledge_linked")) == "FALSE", False)
            .otherwise(None)
        )
    
    # Convert numeric strings to integers
    numeric_columns = ["priority", "impact", "urgency", "reopen_count"]
    for column in numeric_columns:
        if column in df.columns:
            df = df.withColumn(column, col(column).cast("int"))
    
    final_count = df.count()
    removed_count = initial_count - final_count
    
    print(f"[Silver Layer] Records after cleaning: {final_count}")
    print(f"[Silver Layer] Records removed: {removed_count}")
    print(f"[Silver Layer] Transformed schema:")
    df.printSchema()
    
    # Write to silver layer as Parquet
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"[Silver Layer] Data written to: {output_path}")
    
    return df