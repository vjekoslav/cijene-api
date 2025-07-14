import logging
import zipfile
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import time

from service.config import settings
from service.db.stats import compute_stats
from .chain_importer import process_chain

logger = logging.getLogger("importer.archive_handler")

db = settings.get_db()


def parse_date_from_path(path: Path) -> datetime:
    """
    Parse date from path name in YYYY-MM-DD format.
    
    Args:
        path: Path with date in name.
    
    Returns:
        Parsed datetime object.
    
    Raises:
        ValueError: If date format is invalid.
    """
    date_str = path.stem if path.is_file() else path.name
    return datetime.strptime(date_str, "%Y-%m-%d")


async def import_archive(path: Path, compute_stats_flag: bool = True) -> None:
    """Import data from all chain directories in the given zip archive."""
    try:
        price_date = parse_date_from_path(path)
    except ValueError:
        logger.error(f"`{path.stem}` is not a valid date in YYYY-MM-DD format")
        return

    with TemporaryDirectory() as temp_dir:  # type: ignore
        logger.debug(f"Extracting archive {path} to {temp_dir}")
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        await _import(Path(temp_dir), price_date, compute_stats_flag)


async def import_directory(path: Path, compute_stats_flag: bool = True) -> None:
    """Import data from all chain directories in the given directory."""
    if not path.is_dir():
        logger.error(f"`{path}` does not exist or is not a directory")
        return

    try:
        price_date = parse_date_from_path(path)
    except ValueError:
        logger.error(
            f"Directory `{path.name}` is not a valid date in YYYY-MM-DD format"
        )
        return

    await _import(path, price_date, compute_stats_flag)


async def _import(
    path: Path, price_date: datetime, compute_stats_flag: bool = True
) -> None:
    """
    Import data from chain directories in the given path.
    
    Args:
        path: Path containing chain directories.
        price_date: Date for which the prices are valid.
        compute_stats_flag: Whether to compute statistics after import.
    """
    chain_dirs = [d.resolve() for d in path.iterdir() if d.is_dir()]
    if not chain_dirs:
        logger.warning(f"No chain directories found in {path}")
        return

    logger.debug(f"Importing {len(chain_dirs)} chains from {path}")

    t0 = time()

    barcodes = await db.get_product_barcodes()
    for chain_dir in chain_dirs:
        await process_chain(price_date, chain_dir, barcodes)

    dt = int(time() - t0)
    logger.info(f"Imported {len(chain_dirs)} chains in {dt} seconds")

    if compute_stats_flag:
        await compute_stats(price_date)
    else:
        logger.debug(f"Skipping statistics computation for {price_date:%Y-%m-%d}")