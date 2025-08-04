"""
Main script to download and parse CPC data files.

This script orchestrates the download of CPC bulk data files,
parsing of the title list file into a structured format,
and validation of the CPC symbols.
"""

import logging
from pathlib import Path
import polars as pl
from src.cpc_etl.downloader import CPCDownloader
from src.cpc_etl.parser import CPCTitleParser
from src.cpc_etl.validator import CPCValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Main execution function"""
    try:
        # Initialize data directories
        data_dir = Path("data")
        raw_dir = data_dir / "raw"
        processed_dir = data_dir / "processed"

        # Initialize downloader and parser
        downloader = CPCDownloader(data_dir=data_dir)
        parser = CPCTitleParser(output_dir=str(processed_dir))

        # Check file availability
        logger.info("Checking file availability...")
        if not downloader.check_file_availability():
            logger.error("No files available for download")
            return

        # Download files
        logger.info("Starting download process...")
        downloaded_files = downloader.download_bulk_files()
        logger.info(f"Downloaded {len(downloaded_files)} files")

        # Find and process the title list file
        title_list_file = None
        for file in downloaded_files:
            if "TitleList" in file.name:
                title_list_file = file
                break

        if not title_list_file:
            logger.error("Title list file not found in downloaded files")
            return

        # Parse and save title data
        logger.info(f"Processing title list file: {title_list_file}")
        output_path = parser.parse_and_save(
            str(title_list_file), output="cpc_titles.parquet"
        )
        logger.info(f"Successfully saved parsed data to {output_path}")

        # Validate CPC symbols
        logger.info("Validating CPC symbols...")
        validator = CPCValidator(data_dir=raw_dir, version=downloader.version)
        validator.initialize()

        # Load the titles dataframe
        titles_df = pl.read_parquet(output_path)

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
            logger.warning(
                f"Found {len(invalid_symbols)} invalid symbols out of {total_symbols} total symbols:"
            )
            for invalid in invalid_symbols[:10]:  # Show first 10 invalid symbols
                logger.warning(
                    f"Symbol: {invalid['symbol']}, Warnings: {invalid['warnings']}"
                )
            if len(invalid_symbols) > 10:
                logger.warning(
                    f"...and {len(invalid_symbols) - 10} more invalid symbols"
                )
        else:
            logger.info(f"All {total_symbols} symbols are valid!")

            # Save final output with version info
            output_dir = data_dir / "output"
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create output with version date
            version_date = downloader.version
            parquet_path = output_dir / f"cpc_schema_{version_date}.parquet"
            csv_path = output_dir / f"cpc_schema_{version_date}.csv"

            # Add version info to the dataframe
            titles_df = titles_df.with_columns(
                [pl.lit(version_date).alias("cpc_schema_date")]
            )

            # Save final output
            titles_df.write_parquet(parquet_path)
            titles_df.write_csv(csv_path)
            logger.info(f"Saved final output to {parquet_path} and {csv_path}")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
