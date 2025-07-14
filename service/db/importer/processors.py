import logging
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

from service.config import settings
from service.db.models import ChainProduct, Price, Store
from .csv_reader import read_csv

logger = logging.getLogger("importer.processors")

db = settings.get_db()


async def process_stores(stores_path: Path, chain_id: int) -> Dict[str, int]:
    """
    Process stores CSV and import to database.

    Args:
        stores_path: Path to the stores CSV file.
        chain_id: ID of the chain to which these stores belong.

    Returns:
        A dictionary mapping store codes to their database IDs.
    """
    logger.debug(f"Importing stores from {stores_path}")

    stores_data = await read_csv(stores_path)
    
    # Prepare all stores for bulk insertion
    stores_to_create = []
    for store_row in stores_data:
        store = Store(
            chain_id=chain_id,
            code=store_row["store_id"],
            type=store_row.get("type"),
            address=store_row.get("address"),
            city=store_row.get("city"),
            zipcode=store_row.get("zipcode"),
        )
        stores_to_create.append(store)

    # Insert all stores in bulk
    store_map = await db.add_many_stores(stores_to_create)

    logger.debug(f"Processed {len(stores_data)} stores")
    return store_map


def clean_barcode(data: Dict[str, Any], chain_code: str) -> Dict[str, Any]:
    """
    Clean and validate barcode data.
    
    Args:
        data: Product data dictionary.
        chain_code: Code of the retail chain.
    
    Returns:
        Updated product data dictionary.
    """
    barcode = data.get("barcode", "").strip()

    if ":" in barcode:
        return data

    if len(barcode) >= 8 and barcode.isdigit():
        return data

    product_id = data.get("product_id", "")
    if not product_id:
        logger.warning(f"Product has no barcode: {data}")
        return data

    # Construct a chain-specific barcode
    data["barcode"] = f"{chain_code}:{product_id}"
    return data


async def process_products(
    products_path: Path,
    chain_id: int,
    chain_code: str,
    barcodes: Dict[str, int],
) -> Dict[str, int]:
    """
    Process products CSV and import to database.

    As a side effect, this function will also add any newly
    created EAN codes to the provided `barcodes` dictionary.

    Args:
        products_path: Path to the products CSV file.
        chain_id: ID of the chain to which these products belong.
        chain_code: Code of the retail chain.
        barcodes: Dictionary mapping EAN codes to global product IDs.

    Returns:
        A dictionary mapping product codes to their database IDs for the chain.
    """
    logger.debug(f"Processing products from {products_path}")

    products_data = await read_csv(products_path)
    chain_product_map = await db.get_chain_product_map(chain_id)

    # Ideally the CSV would already have valid barcodes, but some older
    # archives contain invalid ones so we need to clean them up.
    new_products = [
        clean_barcode(p, chain_code)
        for p in products_data
        if p["product_id"] not in chain_product_map
    ]

    if not new_products:
        return chain_product_map

    logger.debug(
        f"Found {len(new_products)} new products out of {len(products_data)} total"
    )

    # Collect all new barcodes that don't exist in the current barcodes dict
    new_barcodes = []
    for product in new_products:
        barcode = product["barcode"]
        if barcode not in barcodes:
            new_barcodes.append(barcode)

    # Add all new barcodes in bulk
    if new_barcodes:
        new_barcode_ids = await db.add_many_eans(new_barcodes)
        barcodes.update(new_barcode_ids)
        logger.debug(f"Added {len(new_barcodes)} new barcodes to global products")

    products_to_create = []
    for product in new_products:
        barcode = product["barcode"]
        code = product["product_id"]
        global_product_id = barcodes[barcode]

        products_to_create.append(
            ChainProduct(
                chain_id=chain_id,
                product_id=global_product_id,
                code=code,
                name=product["name"],
                brand=(product["brand"] or "").strip() or None,
                category=(product["category"] or "").strip() or None,
                unit=(product["unit"] or "").strip() or None,
                quantity=(product["quantity"] or "").strip() or None,
            )
        )

    n_inserts = await db.add_many_chain_products(products_to_create)
    if n_inserts != len(new_products):
        logger.warning(
            f"Expected to insert {len(new_products)} products, but inserted {n_inserts}."
        )
    logger.debug(f"Imported {len(new_products)} new chain products")

    chain_product_map = await db.get_chain_product_map(chain_id)
    return chain_product_map


def clean_price(value: str) -> Decimal | None:
    """
    Clean and validate price value.
    
    Args:
        value: Price value as string.
    
    Returns:
        Cleaned price as Decimal or None if invalid.
    """
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    dval = Decimal(value)
    if dval == 0:
        return None
    return dval


async def process_prices(
    price_date: date,
    prices_path: Path,
    chain_id: int,
    store_map: Dict[str, int],
    chain_product_map: Dict[str, int],
) -> int:
    """
    Process prices CSV and import to database using direct CSV streaming.

    Args:
        price_date: The date for which the prices are valid.
        prices_path: Path to the prices CSV file.
        chain_id: ID of the chain to which these prices belong.
        store_map: Dictionary mapping store codes to their database IDs.
        chain_product_map: Dictionary mapping product codes to their database IDs.

    Returns:
        The number of prices successfully inserted into the database.
    """
    logger.debug(f"Processing prices directly from CSV: {prices_path}")

    # Use direct CSV streaming for optimal performance
    n_inserted = await db.add_many_prices_direct_csv(
        prices_path, 
        price_date, 
        store_map, 
        chain_product_map
    )
    
    logger.debug(f"Imported {n_inserted} prices using direct CSV streaming")
    return n_inserted