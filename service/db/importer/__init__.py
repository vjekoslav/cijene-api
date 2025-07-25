"""
Import package for processing price data from retail chains.

This package provides a modular structure for importing price data from
directories or zip archives containing CSV files for retail chains.

The package is organized into:
- csv_reader: Utility for reading CSV files
- archive_handler: Handles zip archives and directory imports
- processors: Core processing functions for stores, products, and prices
- chain_importer: Chain-level import coordination
"""

from .csv_reader import read_csv
from .archive_handler import import_archive, import_directory
from .processors import process_stores, process_products, process_prices
from .chain_importer import process_chain

__all__ = [
    "read_csv",
    "import_archive",
    "import_directory",
    "process_stores",
    "process_products", 
    "process_prices",
    "process_chain",
]