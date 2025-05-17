from csv import DictReader
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from logging import getLogger
from typing import Any

import httpx

from .models import Product

logger = getLogger(__name__)


class BaseCrawler:
    """
    Base crawler class with common functionality and interface for all crawlers.
    """

    CHAIN: str
    BASE_URL: str

    TIMEOUT = 30.0
    USER_AGENT = None

    PRICE_MAP: dict[str, tuple[str, bool]]
    """Mapping from CSV column names to price fields and whether they are required."""

    FIELD_MAP: dict[str, tuple[str, bool]]
    """Mapping from CSV column names to non-price fields and whether they are required."""

    def __init__(self):
        self.client = httpx.Client(timeout=30.0, follow_redirects=True)

    def fetch_text(self, url: str) -> str:
        """
        Download a text file (web page or CSV) from the given URL.

        Args:
            url: URL to download

        Returns:
            The content of the file as a string, or an empty string if the download fails.
        """

        logger.debug(f"Fetching {url}")
        try:
            response = self.client.get(url)
            response.raise_for_status()
            return response.text
        except httpx.RequestError as e:
            logger.error(f"Download from {url} failed: {e}", exc_info=True)
            raise

    def read_csv(self, text: str, delimiter: str = ",") -> DictReader:
        return DictReader(text.splitlines(), delimiter=delimiter)  # type: ignore

    @staticmethod
    def parse_price(
        price_str: str | None,
        required: bool = False,
    ) -> Decimal | None:
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

        price_str = (
            price_str.replace("€", "").replace("EUR", "").replace(",", ".").strip()
        )

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

    def fix_csv_row(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Do any cleaning or transformation of the CSV row data here.

        Args:
            data: Dictionary containing the row data

        Returns:
            The cleaned or transformed data
        """
        return data

    def parse_csv_row(self, row: dict) -> Product:
        """
        Parse a single row of CSV data into a Product object.
        """
        data = {}

        for field, (column, is_required) in self.PRICE_MAP.items():
            value = row.get(column)
            try:
                data[field] = self.parse_price(value, is_required)
            except ValueError as err:
                logger.warning(
                    f"Failed to parse {field} from {column}: {err}",
                    exc_info=True,
                )

        for field, (column, is_required) in self.FIELD_MAP.items():
            value = row.get(column, "").strip()
            if not value and is_required:
                raise ValueError(f"Missing required field: {field}")
            data[field] = value

        self.fix_csv_row(data)
        return Product(**data)  # type: ignore

    def parse_csv(self, content: str) -> list[Product]:
        """
        Parses CSV content into Product objects.

        Args:
            content: CSV content as a string

        Returns:
            List of Product objects
        """
        logger.debug("Parsing CSV content")

        products = []
        for row in self.read_csv(content):
            try:
                product = self.parse_csv_row(row)
            except Exception as e:
                logger.warning(f"Failed to parse row: {row}: {str(e)}", exc_info=True)
                continue
            products.append(product)

        logger.debug(f"Parsed {len(products)} products from CSV")
        return products
