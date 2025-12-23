import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.gold.aggregate_data import create_aggregations


class TestGoldLayer:
    """Unit tests for Gold Layer aggregations."""
    
    def test_create_aggregations_function_exists(self):
        """Test that create_aggregations function is defined."""
        assert callable(create_aggregations)
    
    def test_create_aggregations_parameters(self):
        """Test that create_aggregations has correct parameters."""
        import inspect
        sig = inspect.signature(create_aggregations)
        params = list(sig.parameters.keys())
        assert 'spark' in params
        assert 'input_path' in params
        assert 'output_path' in params
    
    def test_module_docstring(self):
        """Test that the function has proper documentation."""
        assert create_aggregations.__doc__ is not None
        assert 'Gold Layer' in create_aggregations.__doc__


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
