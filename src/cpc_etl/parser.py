import re
import zipfile
from pathlib import Path
from typing import Iterator, Dict, Optional, List, Tuple
import polars as pl
from datetime import datetime


class CPCTitleParser:
    def __init__(self, output_dir: str = "data/processed"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.prerelease_dir = self.output_dir / "prereleases"
        self.prerelease_dir.mkdir(parents=True, exist_ok=True)

    def parse_symbol(self, symbol: str) -> Dict[str, Optional[str]]:
        """Parse a CPC symbol into its components."""
        components = {
            "section": None,  # e.g., A
            "subsection": None,  # e.g., A01
            "group": None,  # e.g., A01B
            "subgroup": None,  # e.g., A01B1/00
        }

        if not symbol or symbol.isdigit():
            return components

        # Extract section (A-H, Y)
        if symbol[0].isalpha():
            components["section"] = symbol[0]

        # Extract subsection (e.g., A01)
        if len(symbol) >= 3 and symbol[1:3].isdigit():
            components["subsection"] = symbol[:3]

        # Extract group (e.g., A01B)
        if len(symbol) >= 4 and symbol[3].isalpha():
            components["group"] = symbol[:4]

        # Extract subgroup (full symbol if it contains a slash, e.g., A01B1/00)
        if "/" in symbol:
            components["subgroup"] = symbol

        return components

    def parse_line(self, line: str) -> Optional[Dict]:
        """Parse a single line from the CPC title file."""
        # Skip empty lines
        if not line.strip():
            return None

        # Extract level number if present (for group/subgroup entries)
        level_match = re.match(r"^([A-Z0-9/]+)\s+(\d+)\s+(.+)$", line)
        if level_match:
            symbol, level, title = level_match.groups()
            level = int(level)
        else:
            # Handle section/class/subclass entries (no level number)
            symbol_match = re.match(r"^([A-Z0-9/]+)\s+(.+)$", line)
            if not symbol_match:
                return None
            symbol, title = symbol_match.groups()
            level = None  # No level for section/class/subclass entries

        # Parse the symbol into components
        components = self.parse_symbol(symbol)

        return {
            "symbol": symbol,
            "level": level,
            "title": title,
            "section": components["section"],
            "class": components["subsection"],
            "subclass": components["group"],
        }

    def process_zip_file(
        self, zip_path: str, is_prerelease: bool = False
    ) -> pl.DataFrame:
        """Process the CPC title list zip file and return DataFrame of titles."""
        title_records = []
        prerelease_date = None

        # Extract prerelease date from filename if it's a prerelease file
        if is_prerelease:
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", str(zip_path))
            if date_match:
                prerelease_date = date_match.group(1)

        with zipfile.ZipFile(zip_path) as zf:
            # Process each section file in the zip
            for filename in zf.namelist():
                if not filename.startswith("cpc-section-"):
                    continue

                with zf.open(filename) as f:
                    for line in f:
                        # Decode line and strip whitespace
                        line = line.decode("utf-8").strip()
                        if not line:
                            continue

                        record = self.parse_line(line)
                        if record:
                            title_records.append(record)

        # Convert records to Polars DataFrame
        titles_df = pl.DataFrame(title_records)

        # Ensure consistent column order and types for titles
        titles_df = titles_df.select(
            [
                "symbol",
                pl.col("level").cast(pl.Float64),
                "title",
                "section",
                "class",
                "subclass",
            ]
        )

        # Add prerelease information if applicable
        if is_prerelease and prerelease_date:
            titles_df = titles_df.with_columns(
                [
                    pl.lit(True).alias("is_prerelease"),
                    pl.lit(prerelease_date).alias("prerelease_date"),
                ]
            )
        else:
            titles_df = titles_df.with_columns(
                [
                    pl.lit(False).alias("is_prerelease"),
                    pl.lit(None).cast(pl.Utf8).alias("prerelease_date"),
                ]
            )

        return titles_df

    def parse_and_save(
        self,
        zip_path: str,
        output: str = "cpc_titles.parquet",
        is_prerelease: bool = False,
    ):
        """Process the zip file and save titles as parquet file."""
        titles_df = self.process_zip_file(zip_path, is_prerelease=is_prerelease)

        # Determine output path based on whether it's a prerelease
        if is_prerelease:
            # Extract date from filename for prerelease output
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", str(zip_path))
            if date_match:
                prerelease_date = date_match.group(1)
                output = f"cpc_schema_prerelease_{prerelease_date}.parquet"
                output_path = self.prerelease_dir / output
            else:
                raise ValueError("Could not extract date from prerelease filename")
        else:
            output_path = self.output_dir / output

        # Save titles
        titles_df.write_parquet(output_path)
        print(f"Saved {len(titles_df)} title records to {output_path}")

        return output_path


def main():
    parser = CPCTitleParser()
    zip_path = "data/raw/CPCTitleList202505.zip"
    parser.parse_and_save(zip_path)


if __name__ == "__main__":
    main()
