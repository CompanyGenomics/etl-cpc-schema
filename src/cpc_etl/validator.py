"""
CPC Data Validator

Handles validation of CPC symbols and titles using multiple data sources.
"""

import zipfile
import logging
from pathlib import Path
from typing import Dict, Set, List, Optional
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from collections import defaultdict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class ValidationResult:
    """Stores validation results for a CPC symbol"""

    symbol_valid: bool = False
    in_symbol_list: bool = False
    validity_status: str = "UNKNOWN"
    schema_valid: bool = False
    parent_symbol: Optional[str] = None
    validation_warnings: List[str] = None

    def __post_init__(self):
        if self.validation_warnings is None:
            self.validation_warnings = []

    def to_dict(self) -> Dict:
        return {
            "symbol_valid": self.symbol_valid,
            "in_symbol_list": self.in_symbol_list,
            "validity_status": self.validity_status,
            "schema_valid": self.schema_valid,
            "parent_symbol": self.parent_symbol,
            "validation_warnings": self.validation_warnings,
        }


class CPCValidator:
    """Validates CPC symbols using multiple data sources"""

    def __init__(self, data_dir: Path = None, version: str = None):
        self.data_dir = Path(data_dir or "data/raw")
        self.version = version
        self.valid_symbols: Set[str] = set()
        self.validity_status: Dict[str, str] = {}
        self.schema_hierarchy: Dict[str, str] = {}
        self.initialized = False

        if not self.version:
            raise ValueError("Version must be provided")

    def initialize(self) -> None:
        """Load validation data from files"""
        if self.initialized:
            return

        self._load_symbol_list()
        self._load_validity_file()
        self._load_schema()
        self.initialized = True

    def _load_symbol_list(self) -> None:
        """Load valid symbols from CPCSymbolList"""
        symbol_file = self.data_dir / f"CPCSymbolList{self.version}.zip"
        if not symbol_file.exists():
            logger.warning(f"Symbol list file not found: {symbol_file}")
            return

        logger.info(f"Loading symbols from {symbol_file}")
        with zipfile.ZipFile(symbol_file) as zf:
            files = zf.namelist()
            logger.info(f"Found files in zip: {files}")

            for filename in files:
                if "CPCSymbolList" in filename and filename.endswith(".csv"):
                    logger.info(f"Processing {filename}")
                    with zf.open(filename) as f:
                        # Skip header
                        next(f)
                        for line in f:
                            parts = line.decode("utf-8").strip().split(",")
                            if len(parts) >= 1:
                                # Normalize symbol by removing extra spaces
                                symbol = "".join(parts[0].split())
                                if symbol:
                                    self.valid_symbols.add(symbol)
                                    # Store status
                                    status = parts[-1] if len(parts) > 6 else "UNKNOWN"
                                    self.validity_status[symbol] = (
                                        "ACTIVE" if status == "published" else status
                                    )

        logger.info(f"Loaded {len(self.valid_symbols)} valid symbols")

    def _load_validity_file(self) -> None:
        """Load symbol validity status from CPCValidityFile"""
        validity_file = self.data_dir / f"CPCValidityFile{self.version}.zip"
        if not validity_file.exists():
            logger.warning(f"Validity file not found: {validity_file}")
            return

        logger.info(f"Loading validity data from {validity_file}")
        with zipfile.ZipFile(validity_file) as zf:
            files = zf.namelist()
            logger.info(f"Found files in zip: {files}")

            for filename in files:
                if filename.endswith(".txt"):
                    logger.info(f"Processing {filename}")
                    with zf.open(filename) as f:
                        # Skip header
                        next(f)
                        for line in f:
                            parts = line.decode("utf-8").strip().split("\t")
                            if len(parts) >= 2:
                                # Normalize symbol by removing extra spaces
                                symbol = "".join(parts[0].split())
                                # If there's a valid_from date but no valid_to date, it's active
                                valid_from = parts[1].strip()
                                valid_to = parts[2].strip() if len(parts) > 2 else ""
                                if valid_from and not valid_to:
                                    self.validity_status[symbol] = "ACTIVE"
                                else:
                                    self.validity_status[symbol] = "INACTIVE"

        logger.info(f"Loaded {len(self.validity_status)} validity statuses")

    def _load_schema(self) -> None:
        """Load schema hierarchy from CPCSchemeXML"""
        schema_file = self.data_dir / f"CPCSchemeXML{self.version}.zip"
        if not schema_file.exists():
            logger.warning(f"Schema file not found: {schema_file}")
            return

        logger.info(f"Loading schema from {schema_file}")
        with zipfile.ZipFile(schema_file) as zf:
            files = zf.namelist()
            logger.info(f"Found files in zip: {files}")

            for filename in files:
                if filename.endswith(".xml"):
                    logger.info(f"Processing {filename}")
                    with zf.open(filename) as f:
                        try:
                            tree = ET.parse(f)
                            root = tree.getroot()
                            self._process_schema_element(root)
                        except ET.ParseError as e:
                            logger.error(f"Failed to parse {filename}: {e}")
                            continue

        logger.info(f"Loaded {len(self.schema_hierarchy)} schema relationships")

    def _process_schema_element(self, element: ET.Element, parent: str = None) -> None:
        """Recursively process schema XML elements to build hierarchy"""
        # Look for classification-symbol element
        symbol_elem = element.find("classification-symbol")
        if symbol_elem is not None and symbol_elem.text:
            # Normalize symbol by removing extra spaces
            symbol = "".join(symbol_elem.text.split())
            if parent:
                self.schema_hierarchy[symbol] = parent
            parent = symbol

        # Process all child classification-item elements
        for child in element.findall("classification-item"):
            self._process_schema_element(child, parent)

    def validate_symbol(self, symbol: str) -> ValidationResult:
        """Validate a CPC symbol using all available data sources"""
        self.initialize()

        result = ValidationResult()

        # Basic symbol format validation
        if self._is_valid_symbol_format(symbol):
            result.symbol_valid = True
        else:
            result.validation_warnings.append("Invalid symbol format")

        # Check against symbol list
        if symbol in self.valid_symbols:
            result.in_symbol_list = True
        else:
            result.validation_warnings.append("Symbol not found in symbol list")

        # Check validity status
        result.validity_status = self.validity_status.get(symbol, "UNKNOWN")
        if result.validity_status != "ACTIVE":
            result.validation_warnings.append(
                f"Symbol status: {result.validity_status}"
            )

        # Check schema hierarchy
        parent = self.schema_hierarchy.get(symbol)
        if parent:
            result.schema_valid = True
            result.parent_symbol = parent
        else:
            result.validation_warnings.append("Symbol not found in schema hierarchy")

        return result

    def _is_valid_symbol_format(self, symbol: str) -> bool:
        """Check if symbol follows valid CPC format"""
        if not symbol:
            return False

        # Basic format validation
        if not symbol[0].isalpha():
            return False

        # Check for valid section letter
        if symbol[0] not in "ABCDEFGHY":
            return False

        # Check for valid class number
        if len(symbol) >= 3 and not symbol[1:3].isdigit():
            return False

        return True
