"""
CPC Prerelease Data Downloader

Handles downloading CPC prerelease files from the official source.
"""

import requests
from pathlib import Path
from typing import Dict, List, Optional
import logging
from bs4 import BeautifulSoup
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class CPCPrereleaseDownloader:
    """Handles downloading CPC prerelease data files"""

    def __init__(self, data_dir: Path = None):
        self.base_url = "https://www.cooperativepatentclassification.org"
        self.prerelease_page_url = f"{self.base_url}/CPCRevisions/prereleases"
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw" / "prereleases"

        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # Cache for discovered files
        self._available_files = None

    def discover_available_files(self) -> Dict[str, str]:
        """
        Fetch and parse the prereleases page to find available files.
        Returns a dict mapping file types to their download URLs.
        """
        if self._available_files is not None:
            return self._available_files

        logger.info("Discovering available files from prereleases page")
        response = requests.get(self.prerelease_page_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        self._available_files = {}

        # Find all links that end with .zip
        for link in soup.find_all("a", href=True):
            href = link.get("href")
            if href.endswith(".zip"):
                # Convert relative URL to absolute URL
                if href.startswith("/"):
                    href = f"{self.base_url}{href}"
                # Extract the filename from the URL
                filename = href.split("/")[-1]
                # Extract the date from the filename (YYYY-MM-DD format)
                date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
                if date_match:
                    self._available_files[filename] = href

        logger.info(f"Found {len(self._available_files)} available prerelease files")
        return self._available_files

    def download_prerelease_files(self) -> List[Path]:
        """
        Download all available CPC prerelease files.
        Returns paths to downloaded files.
        """
        logger.info("Starting prerelease data download process")

        # Discover available files
        available_files = self.discover_available_files()
        if not available_files:
            logger.info("No prerelease files found for download")
            return []

        downloaded_paths = []
        for filename, url in available_files.items():
            output_path = self.raw_dir / filename
            logger.info(f"Downloading {filename} from {url}")
            self._download_file(url, output_path)
            downloaded_paths.append(output_path)

        return downloaded_paths

    def _download_file(self, url: str, filepath: Path) -> None:
        """Download a file from URL to filepath"""
        if filepath.exists():
            logger.info(f"File {filepath.name} already exists, skipping download")
            return

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Successfully downloaded {filepath.name}")
        except requests.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            raise

    def check_file_availability(self) -> bool:
        """Check if any prerelease files are available for download"""
        try:
            available_files = self.discover_available_files()
            return len(available_files) > 0
        except Exception as e:
            logger.error(f"Error checking prerelease file availability: {e}")
            return False

    def get_prerelease_dates(self) -> List[str]:
        """Get all available prerelease dates in YYYY-MM-DD format"""
        available_files = self.discover_available_files()
        dates = set()

        for filename in available_files.keys():
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
            if date_match:
                dates.add(date_match.group(1))

        return sorted(list(dates))
