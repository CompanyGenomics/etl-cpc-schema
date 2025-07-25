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
        output_dir = Path(os.getenv('AzureWebJobsScriptRoot', '/home/site/wwwroot'))
        data_dir = output_dir / "data"
        
        # Initialize and run orchestrator
        orchestrator = ETLOrchestrator(data_dir=data_dir)
        output_path = orchestrator.run()
        
        if output_path:
            logging.info(f"Successfully completed ETL pipeline. Output saved to: {output_path}")
        else:
            logging.error("ETL pipeline failed")
            raise Exception("ETL pipeline failed")
            
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        raise
