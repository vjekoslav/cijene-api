"""
Chain importer for processing individual retail chain data.
"""

import logging
from datetime import date
from pathlib import Path

from service.config import settings
from service.db.models import Chain
from .processors import process_stores, process_products, process_prices

logger = logging.getLogger("importer.chain_importer")

db = settings.get_db()


async def process_chain(
    price_date: date,
    chain_dir: Path,
    barcodes: dict[str, int],
) -> None:
    """
    Process a single retail chain and import its data.

    The expected directory structure and CSV columns are documented in
    `crawler/store/archive_info.txt`.

    Note: updates the `barcodes` dictionary with any new EAN codes found
    (see the `process_products` function).

    Args:
        price_date: The date for which the prices are valid.
        chain_dir: Path to the directory containing the chain's CSV files.
        barcodes: Dictionary mapping EAN codes to global product IDs.
    """
    code = chain_dir.name

    stores_path = chain_dir / "stores.csv"
    if not stores_path.exists():
        logger.warning(f"No stores.csv found for chain {code}")
        return

    products_path = chain_dir / "products.csv"
    if not products_path.exists():
        logger.warning(f"No products.csv found for chain {code}")
        return

    prices_path = chain_dir / "prices.csv"
    if not prices_path.exists():
        logger.warning(f"No prices.csv found for chain {code}")
        return

    logger.debug(f"Processing chain: {code}")

    chain = Chain(code=code)
    chain_id = await db.add_chain(chain)

    store_map = await process_stores(stores_path, chain_id)
    chain_product_map = await process_products(products_path, chain_id, code, barcodes)

    n_new_prices = await process_prices(
        price_date,
        prices_path,
        chain_id,
        store_map,
        chain_product_map,
    )

    logger.info(f"Imported {n_new_prices} new prices for {code}")