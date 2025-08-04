"""
ETL Pipeline Orchestrator

This module contains the core ETL pipeline logic that can be used by both
the CLI interface and Azure Function.
"""

import logging
from pathlib import Path
import polars as pl
from typing import List, Optional, Dict, Tuple

from ..downloader import CPCDownloader
from ..prerelease_downloader import CPCPrereleaseDownloader
from ..parser import CPCTitleParser
from ..validator import CPCValidator


class ETLOrchestrator:
    """Orchestrates the ETL pipeline for CPC data processing."""

    def __init__(self, data_dir: Path = Path("data")):
        """
        Initialize the ETL orchestrator.

        Args:
            data_dir: Base directory for data storage
        """
        self.data_dir = data_dir
        self.raw_dir = data_dir / "raw"
        self.processed_dir = data_dir / "processed"
        self.output_dir = data_dir / "output"

        # Create directories if they don't exist
        for dir_path in [
            self.data_dir,
            self.raw_dir,
            self.processed_dir,
            self.output_dir,
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.downloader = CPCDownloader(data_dir=self.data_dir)
        self.prerelease_downloader = CPCPrereleaseDownloader(data_dir=self.data_dir)
        self.parser = CPCTitleParser(output_dir=str(self.processed_dir))

    def process_bulk_data(self) -> Optional[Tuple[Path, pl.DataFrame]]:
        """
        Process bulk CPC data.

        Returns:
            Tuple of (output path, DataFrame) if successful, None otherwise
        """
        # Check file availability
        logging.info("Checking bulk file availability...")
        if not self.downloader.check_file_availability():
            logging.error("No bulk files available for download")
            return None

        # Download files
        logging.info("Starting bulk download process...")
        downloaded_files = self.downloader.download_bulk_files()
        logging.info(f"Downloaded {len(downloaded_files)} bulk files")

        # Find and process the title list file
        title_list_file = None
        for file in downloaded_files:
            if "TitleList" in file.name:
                title_list_file = file
                break

        if not title_list_file:
            logging.error("Title list file not found in downloaded files")
            return None

        # Parse and save title data
        logging.info(f"Processing bulk title list file: {title_list_file}")
        output_path = self.parser.parse_and_save(
            str(title_list_file), output="cpc_titles.parquet"
        )
        logging.info(f"Successfully saved parsed bulk data to {output_path}")

        # Load and validate the titles dataframe
        titles_df = pl.read_parquet(output_path)
        titles_df = self._validate_symbols(titles_df, self.downloader.version)

        # Add version info to the dataframe
        version_date = self.downloader.version
        titles_df = titles_df.with_columns(
            [pl.lit(version_date).alias("cpc_schema_date")]
        )

        # Save final output
        output_path = self.output_dir / f"cpc_schema_{version_date}.parquet"
        titles_df.write_parquet(output_path)
        logging.info(f"Saved final bulk output to {output_path}")

        return output_path, titles_df

    def process_prerelease_data(self) -> List[Tuple[Path, pl.DataFrame]]:
        """
        Process prerelease CPC data.

        Returns:
            List of tuples (output path, DataFrame) for each prerelease file
        """
        results = []

        # Check file availability
        logging.info("Checking prerelease file availability...")
        if not self.prerelease_downloader.check_file_availability():
            logging.info("No prerelease files available for download")
            return results

        # Download files
        logging.info("Starting prerelease download process...")
        downloaded_files = self.prerelease_downloader.download_prerelease_files()
        logging.info(f"Downloaded {len(downloaded_files)} prerelease files")

        # Process each prerelease file
        for file in downloaded_files:
            if "TitleList" in file.name:
                logging.info(f"Processing prerelease title list file: {file}")
                try:
                    # Parse and save prerelease data
                    output_path = self.parser.parse_and_save(
                        str(file), is_prerelease=True
                    )
                    logging.info(
                        f"Successfully saved parsed prerelease data to {output_path}"
                    )

                    # Load and validate the prerelease dataframe
                    titles_df = pl.read_parquet(output_path)
                    titles_df = self._validate_symbols(
                        titles_df, self.downloader.version
                    )

                    results.append((output_path, titles_df))
                except Exception as e:
                    logging.error(f"Error processing prerelease file {file}: {e}")
                    continue

        return results

    def _validate_symbols(self, titles_df: pl.DataFrame, version: str) -> pl.DataFrame:
        """
        Validate CPC symbols in a DataFrame.

        Args:
            titles_df: DataFrame containing CPC symbols
            version: CPC version for validation

        Returns:
            DataFrame with validation results
        """
        logging.info("Validating CPC symbols...")
        validator = CPCValidator(data_dir=self.raw_dir, version=version)
        validator.initialize()

        # Track invalid symbols
        invalid_symbols = []
        total_symbols = len(titles_df)

        # Validate each symbol
        for symbol in titles_df["symbol"]:
            result = validator.validate_symbol(symbol)
            if not (
                result.symbol_valid
                and result.in_symbol_list
                and result.validity_status == "ACTIVE"
            ):
                invalid_symbols.append(
                    {"symbol": symbol, "warnings": result.validation_warnings}
                )

        # Report validation results
        if invalid_symbols:
            logging.warning(
                f"Found {len(invalid_symbols)} invalid symbols out of {total_symbols} total symbols:"
            )
            for invalid in invalid_symbols[:10]:  # Show first 10 invalid symbols
                logging.warning(
                    f"Symbol: {invalid['symbol']}, Warnings: {invalid['warnings']}"
                )
            if len(invalid_symbols) > 10:
                logging.warning(
                    f"...and {len(invalid_symbols) - 10} more invalid symbols"
                )
        else:
            logging.info(f"All {total_symbols} symbols are valid!")

        return titles_df

    def run(self, force_download: bool = False) -> Dict[str, List[Path]]:
        """
        Run the complete ETL pipeline.

        Args:
            force_download: If True, force redownload of files even if they exist

        Returns:
            Dictionary containing paths to output files:
            {
                "bulk": [path to bulk data file],
                "prereleases": [paths to prerelease data files]
            }
        """
        try:
            output_files = {"bulk": [], "prereleases": []}

            # Process bulk data
            bulk_result = self.process_bulk_data()
            if bulk_result:
                output_path, _ = bulk_result
                output_files["bulk"].append(output_path)

            # Process prerelease data
            prerelease_results = self.process_prerelease_data()
            for output_path, _ in prerelease_results:
                output_files["prereleases"].append(output_path)

            return output_files

        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            raise
