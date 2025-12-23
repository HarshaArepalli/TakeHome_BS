"""
Simple test script to verify the pipeline code structure without running Spark.
This validates that all imports work and functions are properly defined.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_imports():
    """Test that all modules can be imported."""
    try:
        from src.utils.spark_session import get_spark_session
        from src.bronze.ingest_raw_data import ingest_raw_data
        from src.silver.transform_data import clean_and_transform, normalize_column_names
        from src.gold.aggregate_data import create_aggregations
        print("✓ All imports successful")
        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False

def test_file_structure():
    """Test that all required files exist."""
    base_path = Path(__file__).parent.parent
    required_files = [
        base_path / "src" / "main.py",
        base_path / "src" / "utils" / "spark_session.py",
        base_path / "src" / "utils" / "schema.py",
        base_path / "src" / "bronze" / "ingest_raw_data.py",
        base_path / "src" / "silver" / "transform_data.py",
        base_path / "src" / "gold" / "aggregate_data.py",
        base_path / "requirements.txt",
        base_path / "README.md",
    ]
    
    all_exist = True
    for file_path in required_files:
        if file_path.exists():
            print(f"✓ {file_path.relative_to(base_path)}")
        else:
            print(f"✗ Missing: {file_path.relative_to(base_path)}")
            all_exist = False
    
    return all_exist

def test_data_file():
    """Test that the CSV data file exists."""
    base_path = Path(__file__).parent.parent
    csv_path = base_path / "data" / "raw" / "servicenow_incidents_10k.csv"
    
    if csv_path.exists():
        print(f"✓ Data file exists: {csv_path.relative_to(base_path)}")
        # Check file size
        size_mb = csv_path.stat().st_size / (1024 * 1024)
        print(f"  File size: {size_mb:.2f} MB")
        return True
    else:
        print(f"✗ Data file missing: {csv_path.relative_to(base_path)}")
        return False

def main():
    """Run all validation tests."""
    print("=" * 80)
    print("Pipeline Structure Validation")
    print("=" * 80)
    
    print("\n1. Testing File Structure:")
    print("-" * 80)
    file_test = test_file_structure()
    
    print("\n2. Testing Data File:")
    print("-" * 80)
    data_test = test_data_file()
    
    print("\n3. Testing Imports:")
    print("-" * 80)
    import_test = test_imports()
    
    print("\n" + "=" * 80)
    if file_test and data_test and import_test:
        print("✓ All validation tests passed!")
        print("\nNext step: Install Java and run the pipeline:")
        print("  brew install openjdk@11  # Install Java")
        print("  python3 src/main.py      # Run the pipeline")
    else:
        print("✗ Some validation tests failed. Please fix the issues above.")
    print("=" * 80)

if __name__ == "__main__":
    main()
