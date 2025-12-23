from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType

def get_servicenow_schema():
    """
    Define the schema for ServiceNow incident data.
    This can be used for explicit schema definition if needed.
    
    Returns:
        StructType: Schema definition for ServiceNow incidents
    """
    return StructType([
        StructField("number", StringType(), True),
        StructField("sys_id", StringType(), True),
        StructField("opened_at", StringType(), True),
        StructField("closed_at", StringType(), True),
        StructField("state", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("impact", StringType(), True),
        StructField("urgency", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("short_description", StringType(), True),
        StructField("description", StringType(), True),
        StructField("assignment_group", StringType(), True),
        StructField("assigned_to", StringType(), True),
        StructField("assigned_to_sys_id", StringType(), True),
        StructField("caller", StringType(), True),
        StructField("caller_sys_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("location", StringType(), True),
        StructField("cmdb_ci", StringType(), True),
        StructField("sla_breached", StringType(), True),
        StructField("reopen_count", StringType(), True),
        StructField("u_source", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("knowledge_linked", StringType(), True),
        StructField("resolution_code", StringType(), True),
        StructField("close_notes", StringType(), True),
    ])