"""
Tests for CPC parser module
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import zipfile
import xml.etree.ElementTree as ET

from cpc_etl.parser import CPCParser


@pytest.fixture
def parser():
    """Fixture for CPCParser instance"""
    return CPCParser()


def test_parse_title_list_xml(parser):
    """Test parsing title list from XML format"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?><root><item symbol="A01B1/00" title="Hand tools"/><item symbol="A01B3/00" title="Ploughs with fixed plough-shares"/></root>"""
    
    with patch('zipfile.ZipFile') as mock_zip:
        # Mock the file list
        mock_zip.return_value.__enter__.return_value.namelist.return_value = ['file.xml']
        
        # Mock the file content
        mock_file = Mock()
        mock_file.read.return_value = xml_content.encode('utf-8')
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock()
        mock_zip.return_value.__enter__.return_value.open.return_value = mock_file
        
        result = parser.parse_title_list(Path("dummy.zip"))
        
        assert result == {
            'A01B1/00': 'Hand tools',
            'A01B3/00': 'Ploughs with fixed plough-shares'
        }


def test_parse_title_list_text(parser):
    """Test parsing title list from text format"""
    text_content = """
A01B1/00    0    Hand tools (edge trimmers for lawns A01G3/06)
A01B1/02    1    Spades; Shovels
A01B3/00    0    Ploughs with fixed plough-shares
"""
    
    with patch('zipfile.ZipFile') as mock_zip:
        # Mock the file list
        mock_zip.return_value.__enter__.return_value.namelist.return_value = ['file.xml']
        
        # Mock the file content and make XML parsing fail
        mock_file = Mock()
        mock_file.read.return_value = text_content.encode('utf-8')
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock()
        mock_zip.return_value.__enter__.return_value.open.return_value = mock_file
        
        result = parser.parse_title_list(Path("dummy.zip"))
        
        assert result == {
            'A01B1/00': 'Hand tools',
            'A01B3/00': 'Ploughs with fixed plough-shares'
        }


def test_parse_definitions(parser):
    """Test parsing definitions from XML format"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?><root><class-definition symbol="A01B1/00"><definition>Hand tools for working the soil</definition></class-definition><class-definition symbol="A01B3/00"><text>Ploughs with fixed shares for general purposes</text></class-definition></root>"""
    
    with patch('zipfile.ZipFile') as mock_zip:
        # Mock the file list
        mock_zip.return_value.__enter__.return_value.namelist.return_value = ['file.xml']
        
        # Mock the file content
        mock_file = Mock()
        mock_file.read.return_value = xml_content.encode('utf-8')
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock()
        mock_zip.return_value.__enter__.return_value.open.return_value = mock_file
        
        result = parser.parse_definitions(Path("dummy.zip"))
        
        assert result == {
            'A01B1/00': 'Hand tools for working the soil',
            'A01B3/00': 'Ploughs with fixed shares for general purposes'
        }


def test_parse_definitions_with_nested_content(parser):
    """Test parsing definitions with nested XML content"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?><root><class-definition symbol="A01B1/00"><definition><p>Hand tools</p><note>Including spades and shovels</note></definition></class-definition></root>"""
    
    with patch('zipfile.ZipFile') as mock_zip:
        # Mock the file list
        mock_zip.return_value.__enter__.return_value.namelist.return_value = ['file.xml']
        
        # Mock the file content
        mock_file = Mock()
        mock_file.read.return_value = xml_content.encode('utf-8')
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock()
        mock_zip.return_value.__enter__.return_value.open.return_value = mock_file
        
        result = parser.parse_definitions(Path("dummy.zip"))
        
        assert result == {
            'A01B1/00': 'Hand tools Including spades and shovels'
        }
