import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.spark_session import get_spark_session
from src.bronze.ingest_raw_data import ingest_raw_data
from src.silver.transform_data import clean_and_transform
from src.gold.aggregate_data import create_aggregations

def main():
    """
    Main pipeline orchestrator.
    Runs Bronze -> Silver -> Gold data pipeline.
    """
    # Define paths
    base_path = Path(__file__).parent.parent
    raw_data_path = base_path / "data" / "raw" / "servicenow_incidents_10k.csv"
    bronze_path = base_path / "data" / "bronze" / "incidents"
    silver_path = base_path / "data" / "silver" / "incidents"
    gold_path = base_path / "data" / "gold" / "ticket_summary"
    
    # Create directories if they don't exist
    for path in [bronze_path.parent, silver_path.parent, gold_path.parent]:
        path.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("Starting Data Engineering Pipeline: Bronze -> Silver -> Gold")
    print("=" * 80)
    
    # Get Spark session
    spark = get_spark_session("ServiceNow Data Pipeline")
    
    try:
        # Bronze Layer: Ingest raw data
        print("\n" + "=" * 80)
        print("BRONZE LAYER: Raw Data Ingestion")
        print("=" * 80)
        ingest_raw_data(spark, str(raw_data_path), str(bronze_path))
        
        # Silver Layer: Clean and transform
        print("\n" + "=" * 80)
        print("SILVER LAYER: Data Cleaning & Transformation")
        print("=" * 80)
        clean_and_transform(spark, str(bronze_path), str(silver_path))
        
        # Gold Layer: Aggregate for analytics
        print("\n" + "=" * 80)
        print("GOLD LAYER: Data Aggregation for Analytics")
        print("=" * 80)
        create_aggregations(spark, str(silver_path), str(gold_path))
        
        print("\n" + "=" * 80)
        print("Pipeline completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nError occurred during pipeline execution: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()