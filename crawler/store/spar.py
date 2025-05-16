import csv
import datetime
import logging
import re
from io import StringIO
from time import time
from typing import Dict, List, Optional, Tuple

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class SparProduct(BaseModel):
    """
    Represents a product from Spar's price list.
    """

    product: str
    product_id: str
    brand: str
    quantity: str
    unit: str
    price: float
    unit_price: float
    best_price_30: float  # Najniža cijena u posljednjih 30 dana
    anchor_price_date: Optional[str] = None  # Datum sidrene cijene
    anchor_price: Optional[float] = None  # Sidrena cijena
    barcode: str
    category: str


class SparStore(BaseModel):
    """
    Represents a Spar store with its products.
    """

    name: str
    store_type: str  # hipermarket or supermarket
    city: str
    street_address: str
    zipcode: str
    items: List[SparProduct] = Field(default_factory=list)


class SparCrawler:
    """
    Crawler for Spar/InterSpar store prices.

    This class handles downloading and parsing price data from Spar's website.
    It fetches the JSON index file, extracts CSV links, downloads and parses
    the CSVs, and returns a list of products.
    """

    BASE_URL = "https://www.spar.hr"

    def __init__(self) -> None:
        """Initialize the Spar crawler."""
        self.client = httpx.Client(timeout=30.0)

    def get_price_list_url(self, date: datetime.date) -> str:
        """
        Generate the URL for the price list index JSON.

        Args:
            date: The date for which to get the price list

        Returns:
            URL for the price list index JSON
        """
        date_str = date.strftime("%Y%m%d")
        return f"{self.BASE_URL}/datoteke_cjenici/Cjenik{date_str}.json"

    def fetch_price_list_index(self, date: datetime.date) -> Dict:
        """
        Fetch the JSON index file with list of CSV files.

        Args:
            date: The date for which to fetch the price list index

        Returns:
            Dictionary containing the price list index data

        Raises:
            httpx.RequestError: If the request fails
        """
        url = self.get_price_list_url(date)
        logger.info(f"Fetching price list index from {url}")
        response = self.client.get(url)
        response.raise_for_status()

        json_data = response.json()
        count = json_data.get("count", 0)

        logger.debug(f"Successfully fetched price list index, found {count} files")
        return json_data

    def to_camel_case(self, text: str) -> str:
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

    def parse_store_from_filename(self, filename: str) -> Optional[SparStore]:
        """
        Extract store information from CSV filename using regex.

        Args:
            filename: Name of the CSV file with store information

        Returns:
            SparStore object with parsed store information, or None if parsing fails
        """
        logger.debug(f"Parsing store information from filename: {filename}")

        try:
            # Regular expression pattern to extract store information
            pattern = r"^(hipermarket|supermarket)_([^_]+)_(.+?)_(\d{4,})_(.+?)_.*$"
            match = re.match(pattern, filename)

            if not match:
                logger.warning(f"Failed to match filename pattern: {filename}")
                return None

            store_type, city, street_address, zipcode, store_name = match.groups()

            # Format the extracted information
            formatted_store_type = self.to_camel_case(store_type)
            formatted_city = self.to_camel_case(city)
            formatted_street_address = self.to_camel_case(
                street_address.replace("_", " ")
            )
            formatted_store_name = self.to_camel_case(store_name)

            store = SparStore(
                name=formatted_store_name,
                store_type=formatted_store_type,
                city=formatted_city,
                street_address=formatted_street_address,
                zipcode=zipcode,
                items=[],
            )

            logger.info(
                f"Parsed store: {store.name}, {store.store_type}, {store.city}, {store.street_address}, {store.zipcode}"
            )
            return store

        except Exception as e:
            logger.error(f"Failed to parse store from filename {filename}: {str(e)}")
            return None

    def download_csv(self, url: str) -> Optional[str]:
        """
        Downloads a CSV file from the given URL and converts from ISO-8859-2 to UTF-8.

        Args:
            url: URL of the CSV file to download

        Returns:
            CSV content as a string, or None if download fails
        """
        logger.debug(f"Downloading CSV from {url}")

        try:
            response = self.client.get(url)
            response.raise_for_status()
            # Convert from ISO-8859-2 to UTF-8
            csv_content = response.content.decode("iso-8859-2")
            logger.debug(f"Successfully downloaded CSV, size: {len(csv_content)} bytes")
            return csv_content
        except Exception as e:
            logger.error(f"Failed to download CSV from {url}: {str(e)}")
            return None

    def parse_csv(self, csv_content: str) -> List[SparProduct]:
        """
        Parses CSV content into SparProduct objects.

        Args:
            csv_content: CSV content as a string

        Returns:
            List of SparProduct objects
        """
        logger.debug("Parsing CSV content")

        products = []
        reader = csv.DictReader(StringIO(csv_content), delimiter=";")

        for row in reader:
            try:
                # Convert potential empty strings to 0.0 for numeric fields
                price = float(row.get("MPC", "0").replace(",", ".") or 0)
                unit_price = float(
                    row.get("cijena za jedinicu mjere", "0").replace(",", ".") or 0
                )
                best_price_30 = float(
                    row.get("Najniža cijena u posljednjih 30 dana", "0").replace(
                        ",", "."
                    )
                    or 0
                )

                # Handle optional anchor price and date
                anchor_price_str = row.get("sidrena cijena na 2.5.2025.", "")
                anchor_price = (
                    float(anchor_price_str.replace(",", "."))
                    if anchor_price_str
                    else None
                )

                anchor_price_date = row.get("datum sidrene cijene", None)

                product = SparProduct(
                    product=row.get("naziv", ""),
                    product_id=row.get("šifra", ""),
                    brand=row.get("marka", ""),
                    quantity=row.get("neto količina", ""),
                    unit=row.get("jedinica mjere", ""),
                    price=price,
                    unit_price=unit_price,
                    best_price_30=best_price_30,
                    anchor_price=anchor_price,
                    anchor_price_date=anchor_price_date,
                    barcode=row.get("barkod", ""),
                    category=row.get("kategorija proizvoda", ""),
                )
                products.append(product)
            except Exception as e:
                logger.warning(f"Failed to parse row: {row}. Error: {str(e)}")
                continue

        logger.debug(f"Parsed {len(products)} products from CSV")
        return products

    def get_all_products(
        self, date: datetime.date
    ) -> Tuple[datetime.date, List[SparStore]]:
        """
        Main method to fetch and parse all products from Spar's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            Tuple with the date and the list of SparStore objects,
            each containing its products.

        Raises:
            ValueError: If the price list index cannot be fetched or parsed
        """
        logger.info(f"Starting Spar product crawl for date {date}")
        t0 = time()

        try:
            # Fetch the price list index
            price_list_index = self.fetch_price_list_index(date)

            # Extract CSV file URLs from the "files" list
            csv_files = price_list_index.get("files", [])
            logger.info(f"Found {len(csv_files)} CSV files in the price list index")

            stores = []

            # Process each CSV file
            for file_info in csv_files:
                try:
                    filename = file_info.get("name", "")
                    url = file_info.get("URL", "")

                    if not url:
                        logger.warning(f"Skipping file {filename} due to missing URL")
                        continue

                    # Parse store information from the filename
                    store = self.parse_store_from_filename(filename)
                    if not store:
                        logger.warning(
                            f"Skipping CSV from {url} due to store parsing failure"
                        )
                        continue

                    # Download CSV
                    csv_content = self.download_csv(url)
                    if not csv_content:
                        logger.warning(
                            f"Skipping CSV from {url} due to download failure"
                        )
                        continue

                    # Parse CSV and add products to the store
                    products = self.parse_csv(csv_content)
                    store.items = products
                    stores.append(store)

                except Exception as e:
                    logger.error(
                        f"Error processing CSV from {file_info.get('URL')}: {str(e)}"
                    )
                    continue

            total_products = sum(len(store.items) for store in stores)
            t1 = time()
            dt = int(t1 - t0)
            logger.info(
                f"Completed Spar crawl for {date} in {dt}s, found {len(stores)} stores with {total_products} total products"
            )
            return date, stores

        except Exception as e:
            logger.error(f"Failed to fetch or parse Spar price list: {str(e)}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # Example usage with a specific date
    crawler = SparCrawler()
    # Use current date for testing
    current_date = datetime.date.today()
    price_date, stores = crawler.get_all_products(current_date)
