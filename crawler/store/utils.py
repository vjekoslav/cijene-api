import datetime
import logging
import re
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Optional, overload

logger = logging.getLogger(__name__)


def to_camel_case(text: str) -> str:
    """
    Converts text to camel case and replace any '_' with ' '.

    Args:
        text: Input text

    Returns:
        Text converted to camel case
    """
    if text:
        return text.replace("_", " ").title()
    else:
        return ""


@overload
def parse_price(price_str: str, required: bool = True) -> Decimal: ...


@overload
def parse_price(price_str: str, required: bool = False) -> Decimal | None: ...


def parse_price(price_str: str | None, required: bool = False) -> Decimal | None:
    """
    Parse a price string.

    The string may use either , or . as decimal separator, may omit leading
    zero, and may contain currency symbols "€" or "EUR".

    None is handled the same as empty string - no price information available.

    Args:
        price_str: String representing the price, or None (no price)
        required: If True, raises ValueError if the price is not valid
                  If False, returns None for invalid prices

    Returns:
        Parsed price as a Decimal with 2 decimal places

    Raises:
        ValueError: If required is True and the price is not valid
    """
    if price_str is None:
        price_str = ""

    price_str = price_str.replace("€", "").replace("EUR", "").replace(",", ".").strip()

    if not price_str:
        if required:
            raise ValueError("Price is required")
        else:
            return None

    # Handle missing leading zero
    if price_str.startswith("."):
        price_str = "0" + price_str

    try:
        # Convert to Decimal and round to 2 decimal places
        return Decimal(price_str).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (ValueError, TypeError, InvalidOperation):
        logger.warning(f"Failed to parse price: {price_str}")
        if required:
            raise ValueError(f"Invalid price format: {price_str}")
        else:
            return None


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
