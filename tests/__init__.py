"""
Tests for CPC ETL package
"""

import sys
from pathlib import Path

# Add src directory to Python path for imports
src_dir = Path(__file__).parent.parent / "src"
sys.path.append(str(src_dir))
