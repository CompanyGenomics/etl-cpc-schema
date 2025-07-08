"""
Tests for CPC downloader module
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import requests

from cpc_etl.downloader import CPCDownloader


@pytest.fixture
def downloader():
    """Fixture for CPCDownloader instance"""
    test_data_dir = Path("test_data")
    return CPCDownloader(data_dir=test_data_dir)


def test_init(downloader):
    """Test downloader initialization"""
    assert downloader.base_url == "https://www.cooperativepatentclassification.org/sites/default/files/cpc/bulk/"
    assert downloader.version == "202505"
    assert downloader.data_dir == Path("test_data")
    assert downloader.raw_dir == Path("test_data/raw")


@patch('requests.head')
def test_check_file_availability_success(mock_head, downloader):
    """Test successful file availability check"""
    # Mock successful HTTP response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_head.return_value = mock_response
    
    result = downloader.check_file_availability()
    
    assert result is True
    assert mock_head.call_count == 2  # Two files to check


@patch('requests.head')
def test_check_file_availability_failure(mock_head, downloader):
    """Test file availability check when files not found"""
    # Mock 404 response
    mock_response = Mock()
    mock_response.status_code = 404
    mock_head.return_value = mock_response
    
    result = downloader.check_file_availability()
    
    assert result is False
    assert mock_head.call_count == 1  # Should stop after first failure


@patch('requests.head')
def test_check_file_availability_error(mock_head, downloader):
    """Test file availability check with network error"""
    mock_head.side_effect = requests.RequestException("Network error")
    
    result = downloader.check_file_availability()
    
    assert result is False
    assert mock_head.call_count == 1  # Should stop after first error


@patch('requests.get')
def test_download_bulk_files_success(mock_get, downloader):
    """Test successful file downloads"""
    # Mock successful HTTP response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"test data"]
    mock_get.return_value = mock_response
    
    with patch('builtins.open', mock_open()) as mock_file:
        title_path, definitions_path = downloader.download_bulk_files()
        
        assert title_path == downloader.raw_dir / f"CPCTitleList{downloader.version}.zip"
        assert definitions_path == downloader.raw_dir / f"FullCPCDefinitionXML{downloader.version}.zip"
        assert mock_get.call_count == 2  # Two files to download
        assert mock_file.call_count == 2  # Two files to write


@patch('requests.get')
def test_download_bulk_files_error(mock_get, downloader):
    """Test download with network error"""
    mock_get.side_effect = requests.RequestException("Network error")
    
    with pytest.raises(requests.RequestException):
        downloader.download_bulk_files()
