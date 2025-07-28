"""
CPC Data Downloader

Handles downloading bulk CPC files from the official source.
"""

import requests
from pathlib import Path
from typing import Tuple, List, Dict
import logging
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)


class CPCDownloader:
    """Handles downloading CPC bulk data files"""

    def __init__(self, data_dir: Path = None):
        self.base_url = "https://www.cooperativepatentclassification.org"
        self.bulk_page_url = f"{self.base_url}/cpcSchemeAndDefinitions/bulk"
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw"

        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # Cache for discovered files and version
        self._available_files = None
        self._version = None
        self._available_versions = None

    def get_available_versions(self) -> List[str]:
        """Get all available CPC versions from the download page"""
        if self._available_versions is None:
            # Get the bulk download page
            response = requests.get(self.bulk_page_url)
            response.raise_for_status()

            # Parse the page to find version numbers
            soup = BeautifulSoup(response.text, "html.parser")
            versions = set()

            # Look for version numbers in zip file links
            for link in soup.find_all("a", href=True):
                href = link.get("href")
                if href.endswith(".zip"):
                    # Extract version from filename (e.g., CPCSchemeXML202505.zip -> 202505)
                    match = re.search(r"(\d{6})", href)
                    if match:
                        versions.add(match.group(1))

            if not versions:
                raise RuntimeError("No CPC versions found on download page")

            # Sort versions chronologically
            self._available_versions = sorted(versions)
            logger.info(f"Found versions: {', '.join(self._available_versions)}")

        return self._available_versions

    @property
    def version(self) -> str:
        """Get the latest available CPC version"""
        if self._version is None:
            # Get all available versions
            versions = self.get_available_versions()

            # Check if we have any downloaded files
            existing_files = list(self.raw_dir.glob("*.zip"))
            if existing_files:
                # Extract versions from existing files
                existing_versions = set()
                for file in existing_files:
                    match = re.search(r"(\d{6})", file.name)
                    if match:
                        existing_versions.add(match.group(1))

                # Get the latest existing version
                latest_existing = sorted(existing_versions)[-1]

                # Compare with available versions
                latest_available = versions[-1]
                if latest_available > latest_existing:
                    logger.info(
                        f"New version available: {latest_available} (current: {latest_existing})"
                    )
                    self._version = latest_available
                else:
                    logger.info(f"Using current version: {latest_existing}")
                    self._version = latest_existing
            else:
                # No existing files, use latest available
                self._version = versions[-1]
                logger.info(f"Using latest version: {self._version}")

        return self._version

    def discover_available_files(self) -> Dict[str, str]:
        """
        Fetch and parse the bulk download page to find available files.
        Returns a dict mapping file types to their download URLs.
        """
        if self._available_files is not None:
            return self._available_files

        logger.info("Discovering available files from bulk download page")
        response = requests.get(self.bulk_page_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        self._available_files = {}

        # Find all links that match our version
        for link in soup.find_all("a", href=True):
            href = link.get("href")
            if href.endswith(".zip") and self.version in href:
                # Convert relative URL to absolute URL
                if href.startswith("/"):
                    href = f"{self.base_url}{href}"
                # Extract the file type from the URL
                filename = href.split("/")[-1]
                self._available_files[filename] = href

        logger.info(f"Found {len(self._available_files)} available files")
        return self._available_files

    def download_bulk_files(self) -> List[Path]:
        """
        Download all available CPC files for the current version.
        Returns paths to downloaded files.
        """
        logger.info("Starting bulk data download process")

        # Discover available files
        available_files = self.discover_available_files()
        if not available_files:
            raise RuntimeError("No files found for download")

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
        """Check if any files are available for download"""
        try:
            available_files = self.discover_available_files()
            return len(available_files) > 0
        except Exception as e:
            logger.error(f"Error checking file availability: {e}")
            return False
