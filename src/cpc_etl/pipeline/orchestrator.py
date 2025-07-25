"""
ETL Pipeline Orchestrator

This module contains the core ETL pipeline logic that can be used by both
the CLI interface and Azure Function.
"""

import logging
from pathlib import Path
import polars as pl
from typing import List, Optional

from ..downloader import CPCDownloader
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
        for dir_path in [self.data_dir, self.raw_dir, self.processed_dir, self.output_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        # Initialize components
        self.downloader = CPCDownloader(data_dir=self.data_dir)
        self.parser = CPCTitleParser(output_dir=str(self.processed_dir))
        
    def run(self, force_download: bool = False) -> Optional[Path]:
        """
        Run the complete ETL pipeline.
        
        Args:
            force_download: If True, force redownload of files even if they exist
            
        Returns:
            Path to the output file if successful, None otherwise
        """
        try:
            # Check file availability
            logging.info("Checking file availability...")
            if not self.downloader.check_file_availability():
                logging.error("No files available for download")
                return None

            # Download files
            logging.info("Starting download process...")
            downloaded_files = self.downloader.download_bulk_files(force=force_download)
            logging.info(f"Downloaded {len(downloaded_files)} files")

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
            logging.info(f"Processing title list file: {title_list_file}")
            output_path = self.parser.parse_and_save(
                str(title_list_file),
                output="cpc_titles.parquet"
            )
            logging.info(f"Successfully saved parsed data to {output_path}")

            # Validate CPC symbols
            logging.info("Validating CPC symbols...")
            validator = CPCValidator(data_dir=self.raw_dir, version=self.downloader.version)
            validator.initialize()

            # Load the titles dataframe
            titles_df = pl.read_parquet(output_path)
            
            # Track invalid symbols
            invalid_symbols = []
            total_symbols = len(titles_df)
            
            # Validate each symbol
            for symbol in titles_df['symbol']:
                result = validator.validate_symbol(symbol)
                if not (result.symbol_valid and result.in_symbol_list and result.validity_status == "ACTIVE"):
                    invalid_symbols.append({
                        'symbol': symbol,
                        'warnings': result.validation_warnings
                    })

            # Report validation results
            if invalid_symbols:
                logging.warning(f"Found {len(invalid_symbols)} invalid symbols out of {total_symbols} total symbols:")
                for invalid in invalid_symbols[:10]:  # Show first 10 invalid symbols
                    logging.warning(f"Symbol: {invalid['symbol']}, Warnings: {invalid['warnings']}")
                if len(invalid_symbols) > 10:
                    logging.warning(f"...and {len(invalid_symbols) - 10} more invalid symbols")
            else:
                logging.info(f"All {total_symbols} symbols are valid!")
                
                # Save final output with version info
                version_date = self.downloader.version
                output_path = self.output_dir / f"cpc_schema_{version_date}.parquet"
                
                # Add version info to the dataframe
                titles_df = titles_df.with_columns([
                    pl.lit(version_date).alias('cpc_schema_date')
                ])
                
                # Save final output
                titles_df.write_parquet(output_path)
                logging.info(f"Saved final output to {output_path}")
                return output_path

        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            raise

        return None
