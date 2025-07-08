# CPC Schema and Definitions ETL

This project provides an ETL (Extract, Transform, Load) process for fetching, processing, and storing CPC (Cooperative Patent Classification) schema and definitions from the official bulk data source.

## Overview

The ETL process performs the following tasks:
1. **Request bulk CPC schema and definitions** from the official CPC website
2. **Extract information** from the downloaded ZIP files containing XML data
3. **Generate a table** with the format: `cpc_code`, `cpc_name`, `cpc_description`, `cpc_schema_version`
4. **Save the table** to a CSV file for further use

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
uv run main.py
```

The script will:
1. Download the latest CPC Title List and Definitions from the official source
2. Extract and parse the XML data
3. Combine titles and definitions
4. Generate a CSV file in the `data/output/` directory

## Output

The process generates a CSV file named `cpc_data_YYYYMM.csv` with the following columns:

- **cpc_code**: The CPC classification code (e.g., "A01B1/00")
- **cpc_name**: The title/name of the classification
- **cpc_description**: Detailed description from the definitions
- **cpc_schema_version**: Version identifier (e.g., "2025.05")

## Data Sources

The ETL process fetches data from:
- **CPC Title List**: Contains CPC codes and their corresponding titles
- **CPC Definitions**: Contains detailed descriptions for CPC codes

Both are downloaded from: https://www.cooperativepatentclassification.org/cpcSchemeAndDefinitions/bulk

## Project Structure

```
etl-cpc-schema/
├── main.py              # Main ETL script
├── pyproject.toml       # Project configuration and dependencies
├── data/
│   ├── raw/            # Downloaded ZIP files
│   └── output/         # Generated CSV files
├── README.md           # This file
└── tasks.md           # Project requirements
```

## Features

- **Automatic file download**: Downloads the latest CPC data files
- **Robust XML parsing**: Handles various XML formats and structures
- **Error handling**: Graceful handling of network issues and parsing errors
- **Logging**: Detailed logging of the ETL process
- **Data validation**: Ensures data quality and completeness
- **Incremental updates**: Skips downloads if files already exist

## Configuration

The current version is set to "202505" (2025.05) in the `CPCETLProcessor` class. This can be updated as new versions become available.

## Troubleshooting

If you encounter issues:

1. **Network errors**: Check your internet connection and firewall settings
2. **Parsing errors**: The XML structure may have changed; check the logs for details
3. **Missing data**: Some CPC codes may not have corresponding definitions or titles

## License

This project is provided as-is for educational and research purposes.
