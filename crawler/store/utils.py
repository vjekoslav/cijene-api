import datetime
import logging
import re
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Optional

logger = logging.getLogger(__name__)


def to_camel_case(text: str) -> str:
    """
    Converts text to camel case.

    Args:
        text: Input text, typically in lowercase with underscores

    Returns:
        Text converted to camel case
    """
    if not text:
        return ""

    # Replace underscores with spaces
    text = text.replace("_", " ")
    # Split by spaces and capitalize each word
    words = [word.capitalize() for word in text.split()]
    # Join with spaces
    return " ".join(words)


def parse_price(price_str: str) -> Decimal:
    """
    Parse a price string that may use either , or . as decimal separator.

    Args:
        price_str: String representing a price, possibly with "," as decimal separator

    Returns:
        Parsed price as a Decimal with 2 decimal places
    """
    if not price_str or price_str.strip() == "":
        return Decimal("0.00")

    # Replace comma with dot for decimal point
    normalized = price_str.replace(",", ".")

    # Handle missing leading zero
    if normalized.startswith("."):
        normalized = "0" + normalized

    try:
        # Convert to Decimal and round to 2 decimal places
        price = Decimal(normalized).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return price
    except (ValueError, InvalidOperation):
        logger.warning(f"Failed to parse price: {price_str}")
        return Decimal("0.00")


def log_operation_timing(
    operation_name: str,
    store_name: str,
    date: datetime.date,
    start_time: float,
    end_time: float,
    store_count: int,
    total_products: int,
) -> None:
    """
    Log the timing information for a crawler operation.

    Args:
        operation_name: Name of the operation being timed
        store_name: Name of the store being crawled
        date: The date for which the crawl was performed
        start_time: The start time in seconds
        end_time: The end time in seconds
        store_count: The number of stores processed
        total_products: The total number of products found
    """
    dt = int(end_time - start_time)
    logger.info(
        f"Completed {store_name} {operation_name} for {date} in {dt}s, "
        f"found {store_count} stores with {total_products} total products"
    )


def extract_zipcode_from_text(text: str) -> Optional[str]:
    """
    Extracts a zipcode (postal code) from text using a regex pattern.

    Args:
        text: Text that might contain a zipcode

    Returns:
        The extracted zipcode or None if not found
    """
    # Common pattern for Croatian zipcodes (5 digits)
    zipcode_pattern = r"\b(\d{5})\b"
    match = re.search(zipcode_pattern, text)
    return match.group(1) if match else None
