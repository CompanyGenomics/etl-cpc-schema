"""
Tests for CPC parser module
"""

import pytest
from pathlib import Path
import zipfile
import polars as pl

from src.cpc_etl.parser import CPCTitleParser


@pytest.fixture
def parser(tmp_path):
    """Fixture for CPCTitleParser instance using temporary directory"""
    return CPCTitleParser(output_dir=str(tmp_path))


def test_init(parser, tmp_path):
    """Test parser initialization"""
    assert parser.output_dir == tmp_path
    assert parser.output_dir.exists()


def test_parse_symbol():
    """Test parsing CPC symbols into components"""
    parser = CPCTitleParser()
    
    # Test various symbol formats
    assert parser.parse_symbol("A") == {
        'section': 'A',
        'subsection': None,
        'group': None,
        'subgroup': None
    }
    
    assert parser.parse_symbol("A01") == {
        'section': 'A',
        'subsection': 'A01',
        'group': None,
        'subgroup': None
    }
    
    assert parser.parse_symbol("A01B") == {
        'section': 'A',
        'subsection': 'A01',
        'group': 'A01B',
        'subgroup': None
    }
    
    assert parser.parse_symbol("A01B1/00") == {
        'section': 'A',
        'subsection': 'A01',
        'group': 'A01B',
        'subgroup': 'A01B1/00'
    }


def test_parse_line():
    """Test parsing individual lines from CPC title file"""
    parser = CPCTitleParser()
    
    # Test section line
    section_line = "A01B1/00 0 Hand tools"
    result = parser.parse_line(section_line)
    assert result == {
        'symbol': 'A01B1/00',
        'level': 0,
        'title': 'Hand tools',
        'section': 'A',
        'class': 'A01',
        'subclass': 'A01B'
    }
    
    # Test empty line
    assert parser.parse_line("") is None
    
    # Test invalid line
    assert parser.parse_line("Invalid Line") is None


def test_process_zip_file(parser):
    """Test processing CPC title list zip file using real data"""
    # Use the actual test data file
    test_zip_path = "tests/data/raw/test_cpc_titles.zip"
    
    # Process the actual zip file
    df = parser.process_zip_file(test_zip_path)
    
    # Verify DataFrame structure and content
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert all(col in df.columns for col in ['symbol', 'level', 'title', 'section', 'class', 'subclass'])
    
    # Verify column types
    schema = df.schema
    assert str(schema['symbol']) == 'String'
    assert str(schema['level']) == 'Float64'
    assert str(schema['title']) == 'String'
    assert str(schema['section']) == 'String'
    assert str(schema['class']) == 'String'
    assert str(schema['subclass']) == 'String'
    
    # Verify actual data content
    # Find a row with a level value for type checking
    row_with_level = None
    for i in range(len(df)):
        row = df.row(i)
        if row[1] is not None:  # Check if level is not None
            row_with_level = row
            break
    
    assert row_with_level is not None  # Ensure we found a row with level
    assert len(row_with_level) == 6  # Should have all 6 columns
    assert isinstance(row_with_level[0], str)  # symbol should be string
    assert isinstance(row_with_level[1], float)  # level should be float
    assert isinstance(row_with_level[2], str)  # title should be string


def test_parse_and_save(parser, tmp_path):
    """Test parsing and saving CPC titles using real data"""
    # Use the actual test data file
    test_zip_path = "tests/data/raw/test_cpc_titles.zip"
    
    # Process and save the data
    output_path = parser.parse_and_save(test_zip_path)
    
    # Verify output
    assert isinstance(output_path, Path)
    assert output_path.exists()
    assert str(output_path).endswith('cpc_titles.parquet')
    
    # Verify the saved data can be read back
    df = pl.read_parquet(output_path)
    assert len(df) > 0
    assert all(col in df.columns for col in ['symbol', 'level', 'title', 'section', 'class', 'subclass'])


def test_parse_line_with_parentheses():
    """Test parsing lines with parenthetical content"""
    parser = CPCTitleParser()
    
    line = "A01B1/00 0 Hand tools (edge trimmers for lawns A01G3/06)"
    result = parser.parse_line(line)
    assert result == {
        'symbol': 'A01B1/00',
        'level': 0,
        'title': 'Hand tools (edge trimmers for lawns A01G3/06)',
        'section': 'A',
        'class': 'A01',
        'subclass': 'A01B'
    }


def test_parse_line_with_semicolons():
    """Test parsing lines with semicolon-separated content"""
    parser = CPCTitleParser()
    
    line = "A01B1/02 1 Spades; Shovels; Hoes"
    result = parser.parse_line(line)
    assert result == {
        'symbol': 'A01B1/02',
        'level': 1,
        'title': 'Spades; Shovels; Hoes',
        'section': 'A',
        'class': 'A01',
        'subclass': 'A01B'
    }


def test_parse_symbol_edge_cases():
    """Test parsing edge cases for CPC symbols"""
    parser = CPCTitleParser()
    
    # Empty string
    assert parser.parse_symbol("") == {
        'section': None,
        'subsection': None,
        'group': None,
        'subgroup': None
    }
    
    # Invalid format - numeric input
    assert parser.parse_symbol("123") == {
        'section': None,
        'subsection': None,
        'group': None,
        'subgroup': None
    }
    
    # Y section (special case)
    assert parser.parse_symbol("Y02E") == {
        'section': 'Y',
        'subsection': 'Y02',
        'group': 'Y02E',
        'subgroup': None
    }
