"""
CLI Interface for CPC ETL Pipeline

This module provides a command-line interface for running the CPC ETL pipeline.
"""

import logging
from pathlib import Path
import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from ..pipeline.orchestrator import ETLOrchestrator

# Initialize typer app
app = typer.Typer(
    name="cpc-etl",
    help="ETL pipeline for CPC schema and definitions",
    add_completion=False,
)

# Configure rich logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)

console = Console()


@app.command()
def run(
    data_dir: Path = typer.Option(
        "data", "--data-dir", "-d", help="Directory for data storage"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Force redownload of files"
    ),
    skip_prereleases: bool = typer.Option(
        False, "--skip-prereleases", help="Skip processing prerelease data"
    ),
):
    """Run the complete ETL pipeline"""
    try:
        console.print("[bold blue]Starting CPC ETL Pipeline...[/]")

        # Initialize and run orchestrator
        orchestrator = ETLOrchestrator(data_dir=data_dir)
        output_files = orchestrator.run(force_download=force)

        if output_files:
            console.print(f"[bold green]Successfully completed ETL pipeline![/]")

            # Create a table to display output files
            table = Table(title="Output Files")
            table.add_column("Type", style="cyan")
            table.add_column("Path", style="green")

            # Add bulk data files
            for path in output_files["bulk"]:
                table.add_row("Bulk", str(path))

            # Add prerelease data files
            if not skip_prereleases:
                for path in output_files["prereleases"]:
                    table.add_row("Prerelease", str(path))

            console.print(table)
        else:
            console.print("[bold red]ETL pipeline failed[/]")
            raise typer.Exit(code=1)

    except Exception as e:
        console.print(f"[bold red]Error:[/] {str(e)}")
        raise typer.Exit(code=1)


def main():
    """Entry point for the CLI"""
    app()


if __name__ == "__main__":
    main()
