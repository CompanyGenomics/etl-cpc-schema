"""
Tests for CPC downloader module
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import requests
from bs4 import BeautifulSoup

from src.cpc_etl.downloader import CPCDownloader


@pytest.fixture
def downloader():
    """Fixture for CPCDownloader instance"""
    test_data_dir = Path("test_data")
    return CPCDownloader(data_dir=test_data_dir)


def test_init(downloader):
    """Test downloader initialization"""
    assert downloader.base_url == "https://www.cooperativepatentclassification.org"
    assert downloader.bulk_page_url == f"{downloader.base_url}/cpcSchemeAndDefinitions/bulk"
    assert downloader.data_dir == Path("test_data")
    assert downloader.raw_dir == Path("test_data/raw")


@patch('requests.get')
def test_get_available_versions(mock_get, downloader):
    """Test getting available CPC versions"""
    # Create mock HTML content
    html_content = """
    <html>
        <body>
            <a href="/files/CPCSchemeXML202401.zip">January 2024</a>
            <a href="/files/CPCSchemeXML202505.zip">May 2025</a>
            <a href="/files/CPCSchemeXML202503.zip">March 2025</a>
        </body>
    </html>
    """
    mock_response = Mock()
    mock_response.text = html_content
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    
    versions = downloader.get_available_versions()
    
    assert versions == ['202401', '202503', '202505']
    mock_get.assert_called_once_with(downloader.bulk_page_url)


@patch('requests.get')
def test_discover_available_files(mock_get, downloader):
    """Test discovering available files"""
    # Create mock HTML content
    html_content = """
    <html>
        <body>
            <a href="/files/CPCSchemeXML202505.zip">Scheme</a>
            <a href="/files/CPCTitleList202505.zip">Titles</a>
        </body>
    </html>
    """
    mock_response = Mock()
    mock_response.text = html_content
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    
    # Set version to avoid additional requests
    downloader._version = '202505'
    
    files = downloader.discover_available_files()
    
    assert len(files) == 2
    assert 'CPCSchemeXML202505.zip' in files
    assert 'CPCTitleList202505.zip' in files


@patch('requests.get')
def test_version_property_no_existing_files(mock_get, downloader):
    """Test version property with no existing files"""
    # Mock available versions response
    html_content = '<a href="/files/CPCSchemeXML202505.zip">Latest</a>'
    mock_response = Mock()
    mock_response.text = html_content
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    
    assert downloader.version == '202505'


@patch('requests.get')
def test_download_bulk_files_success(mock_get, downloader):
    """Test successful file downloads"""
    # Mock file discovery
    downloader._version = '202505'
    downloader._available_files = {
        'CPCSchemeXML202505.zip': 'http://example.com/scheme.zip',
        'CPCTitleList202505.zip': 'http://example.com/titles.zip'
    }
    
    # Mock successful HTTP response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"test data"]
    mock_get.return_value = mock_response
    
    with patch('builtins.open', mock_open()) as mock_file:
        paths = downloader.download_bulk_files()
        
        assert len(paths) == 2
        assert all(isinstance(p, Path) for p in paths)
        assert mock_get.call_count == 2
        assert mock_file.call_count == 2


@patch('requests.get')
def test_download_bulk_files_error(mock_get, downloader):
    """Test download with network error"""
    # Mock file discovery
    downloader._version = '202505'
    downloader._available_files = {
        'CPCSchemeXML202505.zip': 'http://example.com/scheme.zip'
    }
    
    mock_get.side_effect = requests.RequestException("Network error")
    
    with pytest.raises(requests.RequestException):
        downloader.download_bulk_files()


def test_check_file_availability_with_files(downloader):
    """Test file availability check with discovered files"""
    # Mock discovered files
    downloader._version = '202505'
    downloader._available_files = {
        'CPCSchemeXML202505.zip': 'http://example.com/scheme.zip',
        'CPCTitleList202505.zip': 'http://example.com/titles.zip'
    }
    
    assert downloader.check_file_availability() is True


@patch('requests.get')
def test_check_file_availability_error(mock_get, downloader):
    """Test file availability check with error"""
    mock_get.side_effect = requests.RequestException("Network error")
    
    assert downloader.check_file_availability() is False
