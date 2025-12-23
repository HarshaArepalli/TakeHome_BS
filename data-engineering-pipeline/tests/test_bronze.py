import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.bronze.ingest_raw_data import ingest_raw_data


class TestBronzeLayer:
    """Unit tests for Bronze Layer ingestion."""
    
    def test_ingest_raw_data_function_exists(self):
        """Test that ingest_raw_data function is defined."""
        assert callable(ingest_raw_data)
    
    def test_ingest_raw_data_parameters(self):
        """Test that ingest_raw_data has correct parameters."""
        import inspect
        sig = inspect.signature(ingest_raw_data)
        params = list(sig.parameters.keys())
        assert 'spark' in params
        assert 'input_path' in params
        assert 'output_path' in params
    
    def test_module_docstring(self):
        """Test that the function has proper documentation."""
        assert ingest_raw_data.__doc__ is not None
        assert 'Bronze Layer' in ingest_raw_data.__doc__


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
