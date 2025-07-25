"""
Import package for processing price data from retail chains.

This package provides utilities for importing price data from
directories or zip archives containing CSV files for retail chains.

The package is organized into:
- csv_reader: Utility for reading CSV files
"""

from .csv_reader import read_csv

__all__ = [
    "read_csv",
]