"""
Azure Function Implementation for CPC ETL Pipeline

This module implements the Azure Function that runs the CPC ETL pipeline
on a scheduled basis.
"""

import azure.functions as func
import logging
from pathlib import Path
import os

from ..pipeline.orchestrator import ETLOrchestrator


def main(mytimer: func.TimerRequest) -> None:
    """
    Azure Function entry point.

    This function runs on a schedule (defined in function.json) and
    executes the ETL pipeline.
    """
    try:
        # Get the output container path from environment variable
        output_dir = Path(os.getenv("AzureWebJobsScriptRoot", "/home/site/wwwroot"))
        data_dir = output_dir / "data"

        # Initialize and run orchestrator
        orchestrator = ETLOrchestrator(data_dir=data_dir)
        output_files = orchestrator.run()

        if output_files:
            # Log bulk data files
            for path in output_files["bulk"]:
                logging.info(
                    f"Successfully processed bulk data. Output saved to: {path}"
                )

            # Log prerelease data files
            for path in output_files["prereleases"]:
                logging.info(
                    f"Successfully processed prerelease data. Output saved to: {path}"
                )

            if not output_files["bulk"] and not output_files["prereleases"]:
                logging.warning("No files were processed")
        else:
            logging.error("ETL pipeline failed")
            raise Exception("ETL pipeline failed")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        raise
