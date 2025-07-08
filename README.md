# CPC Schema and Definitions ETL

This project provides an ETL (Extract, Transform, Load) process for fetching, processing, and validating CPC (Cooperative Patent Classification) schema and definitions from the official bulk data source.

## Overview

The ETL process performs the following tasks:
1. **Request bulk CPC schema and definitions** from the official CPC website
2. **Extract information** from the downloaded ZIP files containing XML data
3. **Parse and validate** CPC symbols against the official schema
4. **Generate structured data** with validated CPC codes, titles, and version information
5. **Save the data** in Parquet format for efficient storage and querying

## Requirements

- Python 3.13+
- uv package manager

## Installation

1. Clone or download this repository
2. Install dependencies using uv:

```bash
uv sync
```

## Usage

Run the ETL process:

```bash
uv run python main.py
```

The script will:
1. Check availability of CPC bulk data files
2. Download the latest CPC Title List and Definitions
3. Extract and parse the XML data
4. Validate CPC symbols against the official schema
5. Generate a Parquet file in the `data/output/` directory

## Output

The process generates a Parquet file named `cpc_schema_YYYYMM.parquet` with the following columns:

- **symbol**: The CPC classification code (e.g., "A01B1/00")
- **title**: The title/name of the classification
- **cpc_schema_date**: Version identifier (e.g., "202505")

## Data Sources

The ETL process fetches data from:
- **CPC Title List**: Contains CPC codes and their corresponding titles
- **CPC Definitions**: Contains detailed descriptions for CPC codes

Both are downloaded from: https://www.cooperativepatentclassification.org/cpcSchemeAndDefinitions/bulk

## Project Structure

```
etl-cpc-schema/
├── src/
│   └── cpc_etl/
│       ├── __init__.py
│       ├── downloader.py    # Handles file downloads
│       ├── parser.py        # Parses XML data
│       └── validator.py     # Validates CPC symbols
├── tests/
│   ├── __init__.py
│   ├── test_downloader.py
│   ├── test_parser.py
│   └── data/
│       └── raw/           # Test data files
├── data/
│   ├── raw/              # Downloaded ZIP files
│   ├── processed/        # Intermediate processed files
│   └── output/          # Final output files
├── main.py              # Main ETL script
├── pyproject.toml       # Project configuration and dependencies
├── README.md           # This file
└── tasks.md            # Project requirements
```

## Features

- **Automatic file download**: Downloads the latest CPC data files
- **Robust XML parsing**: Handles various XML formats and structures
- **Comprehensive validation**: Validates CPC symbols against official schema
- **Error handling**: Graceful handling of network issues and parsing errors
- **Detailed logging**: Tracks progress and issues throughout the ETL process
- **Efficient storage**: Uses Parquet format for optimized data storage
- **Incremental updates**: Skips downloads if files already exist

## Validation Process

The ETL process includes a thorough validation step that:
1. Checks if each CPC symbol follows the correct format
2. Verifies symbols against the official CPC symbol list
3. Confirms symbols are currently active in the schema
4. Logs detailed warnings for any invalid symbols
5. Provides statistics on validation results

## Troubleshooting

If you encounter issues:

1. **Network errors**: 
   - Check your internet connection
   - Verify firewall settings
   - Ensure access to the CPC bulk data website

2. **Parsing errors**: 
   - Check logs for specific XML parsing issues
   - Verify downloaded files are not corrupted
   - Ensure sufficient disk space for extraction

3. **Validation failures**:
   - Review logged validation warnings
   - Check if symbols match current CPC schema version
   - Verify symbol format follows CPC standards

## License

This project is provided as-is for educational and research purposes.
