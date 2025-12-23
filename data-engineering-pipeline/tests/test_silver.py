import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.silver.transform_data import clean_and_transform, normalize_column_names


class TestSilverLayer:
    """Unit tests for Silver Layer transformation."""
    
    def test_clean_and_transform_function_exists(self):
        """Test that clean_and_transform function is defined."""
        assert callable(clean_and_transform)
    
    def test_normalize_column_names_function_exists(self):
        """Test that normalize_column_names function is defined."""
        assert callable(normalize_column_names)
    
    def test_clean_and_transform_parameters(self):
        """Test that clean_and_transform has correct parameters."""
        import inspect
        sig = inspect.signature(clean_and_transform)
        params = list(sig.parameters.keys())
        assert 'spark' in params
        assert 'input_path' in params
        assert 'output_path' in params
    
    def test_normalize_column_names_parameters(self):
        """Test that normalize_column_names has correct parameters."""
        import inspect
        sig = inspect.signature(normalize_column_names)
        params = list(sig.parameters.keys())
        assert 'df' in params
    
    def test_module_docstring(self):
        """Test that functions have proper documentation."""
        assert clean_and_transform.__doc__ is not None
        assert 'Silver Layer' in clean_and_transform.__doc__


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
