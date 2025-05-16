import csv
import datetime
import logging
import re
import tempfile
import zipfile
from decimal import Decimal
from io import StringIO
from time import time
from typing import List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup

from crawler.store.models import Store, Product
from crawler.store.utils import to_camel_case, parse_price, log_operation_timing

logger = logging.getLogger(__name__)


class PlodineCrawler:
    """
    Crawler for Plodine store prices.

    This class handles downloading and parsing price data from Plodine's website.
    It fetches the price list index page, finds the ZIP for the specified date,
    downloads and extracts it, and parses the CSV files inside.
    """

    BASE_URL = "https://www.plodine.hr"
    PRICE_LIST_URL = f"{BASE_URL}/info-o-cijenama"

    def __init__(self) -> None:
        """Initialize the Plodine crawler."""
        self.client = httpx.Client(timeout=60.0)  # Longer timeout for ZIP download

    def fetch_index(self) -> str:
        """
        Fetches the price list index page from Plodine's website.

        Returns:
            str: HTML content of the price list page

        Raises:
            httpx.RequestError: If the request fails
        """
        logger.info(f"Fetching price list index page from {self.PRICE_LIST_URL}")
        response = self.client.get(self.PRICE_LIST_URL)
        response.raise_for_status()
        logger.debug(
            f"Successfully fetched price list index page, size: {len(response.text)} bytes"
        )
        return response.text

    def find_zip_url_for_date(
        self, html_content: str, target_date: datetime.date
    ) -> Optional[str]:
        """
        Parse HTML to find ZIP file URL for the specified date.

        Args:
            html_content: HTML content of the price list index page
            target_date: Date for which to find the ZIP file

        Returns:
            Optional[str]: URL of the ZIP file for the specified date, or None if not found
        """
        logger.debug(f"Looking for ZIP file for date: {target_date}")
        soup = BeautifulSoup(html_content, "html.parser")

        # Format date for comparison (DD.MM.YYYY.)
        target_date_str = target_date.strftime("%d.%m.%Y.")

        # Find all links on the page
        links = soup.find_all("a", href=True)

        for link in links:
            # Check if link text contains the target date
            if target_date_str in link.text:
                zip_url = link["href"]
                if zip_url.endswith(".zip"):
                    logger.info(f"Found ZIP URL for {target_date_str}: {zip_url}")
                    return zip_url

        logger.warning(f"No ZIP file found for date: {target_date_str}")
        return None

    def download_and_extract_zip(self, zip_url: str) -> List[Tuple[str, str]]:
        """
        Download ZIP file to temp location, extract CSVs, return list of (filename, content).

        Args:
            zip_url: URL of the ZIP file to download

        Returns:
            List[Tuple[str, str]]: List of tuples with (filename, content) for each CSV in the ZIP

        Raises:
            httpx.RequestError: If the ZIP download fails
            zipfile.BadZipFile: If the ZIP file is corrupted
        """
        logger.info(f"Downloading ZIP file from {zip_url}")
        response = self.client.get(zip_url)
        response.raise_for_status()

        logger.debug(
            f"Successfully downloaded ZIP, size: {len(response.content)} bytes"
        )

        # Create a temporary file to store the ZIP
        with tempfile.NamedTemporaryFile() as temp_zip:
            temp_zip.write(response.content)
            temp_zip.flush()

            csv_files = []
            try:
                with zipfile.ZipFile(temp_zip.name, "r") as zip_ref:
                    # List all CSV files in the ZIP
                    csv_filenames = [
                        f for f in zip_ref.namelist() if f.lower().endswith(".csv")
                    ]
                    logger.info(f"Found {len(csv_filenames)} CSV files in the ZIP")

                    # Extract and read each CSV file
                    for filename in csv_filenames:
                        try:
                            with zip_ref.open(filename) as f:
                                # Decode from ISO-8859-2 (common encoding for Croatian text)
                                content = f.read().decode("iso-8859-2")
                                csv_files.append((filename, content))
                        except Exception as e:
                            logger.error(
                                f"Failed to extract or decode CSV file {filename}: {str(e)}"
                            )
                            continue
            except zipfile.BadZipFile as e:
                logger.error(f"Invalid ZIP file: {str(e)}")
                raise

        logger.debug(f"Successfully extracted {len(csv_files)} CSV files from ZIP")
        return csv_files

    def parse_store_from_filename(self, filename: str) -> Optional[Store]:
        """
        Extract store information from CSV filename using regex.

        Args:
            filename: Name of the CSV file with store information

        Returns:
            Store object with parsed store information, or None if parsing fails
        """
        logger.debug(f"Parsing store information from filename: {filename}")

        try:
            # Regular expression pattern to extract store information
            # Pattern for files like: SUPERMARKET_ULICA_FRANJE_TUDJMANA_83A_10450_JASTREBARSKO_063_2_16052025020937.csv
            pattern = r"^(SUPERMARKET|HIPERMARKET)_(.+?)_(\d{5})_([^_]+)_(\d+)_.*\.csv$"
            match = re.match(pattern, filename)

            if not match:
                logger.warning(f"Failed to match filename pattern: {filename}")
                return None

            store_type, street_address_raw, zipcode, city, store_id = match.groups()

            # Format the extracted information
            formatted_store_type = to_camel_case(store_type)
            formatted_street_address = to_camel_case(street_address_raw)
            formatted_city = to_camel_case(city)

            # Use city as the store name
            store_name = f"Plodine {formatted_city}"

            store = Store(
                chain="plodine",
                store_id=store_id,
                name=store_name,
                store_type=formatted_store_type,
                city=formatted_city,
                street_address=formatted_street_address,
                zipcode=zipcode,
                items=[],
            )

            logger.info(
                f"Parsed store: {store.name} ({store.store_id}), {store.store_type}, {store.city}, {store.street_address}, {store.zipcode}"
            )
            return store

        except Exception as e:
            logger.error(f"Failed to parse store from filename {filename}: {str(e)}")
            return None

    def parse_csv(self, csv_content: str) -> List[Product]:
        """
        Parses CSV content into Product objects.

        Args:
            csv_content: CSV content as a string

        Returns:
            List of Product objects
        """
        logger.debug("Parsing CSV content")

        products = []
        reader = csv.DictReader(StringIO(csv_content), delimiter=";")

        for row in reader:
            try:
                # Parse prices, handling different decimal separators
                price = parse_price(row.get("Maloprodajna cijena", ""))
                unit_price = parse_price(row.get("Cijena po JM", ""))
                special_price_str = row.get(
                    "MPC za vrijeme posebnog oblika prodaje", ""
                )
                special_price = (
                    parse_price(special_price_str) if special_price_str else None
                )
                best_price_30 = parse_price(
                    row.get("Najniza cijena u poslj. 30 dana", "")
                )
                anchor_price_str = row.get("Sidrena cijena na 2.5.2025", "")
                anchor_price = (
                    parse_price(anchor_price_str) if anchor_price_str else None
                )

                product = Product(
                    product=row.get("Naziv proizvoda", ""),
                    product_id=row.get("Sifra proizvoda", ""),
                    brand=row.get("Marka proizvoda", ""),
                    quantity=row.get("Neto kolicina", ""),
                    unit=row.get("Jedinica mjere", ""),
                    price=price,
                    unit_price=unit_price,
                    special_price=special_price,
                    best_price_30=best_price_30,
                    anchor_price=anchor_price,
                    barcode=row.get("Barkod", ""),
                    category=row.get("Kategorija proizvoda", ""),
                )
                products.append(product)
            except Exception as e:
                logger.warning(f"Failed to parse row: {row}. Error: {str(e)}")
                continue

        logger.debug(f"Parsed {len(products)} products from CSV")
        return products

    def get_csv_files_for_date(self, date: datetime.date) -> List[Tuple[str, str]]:
        """
        Fetches the index page, finds the ZIP URL for the given date,
        downloads the ZIP, and extracts CSV files.

        Args:
            date: The date for which to fetch data.

        Returns:
            A list of tuples, where each tuple contains the filename and content of a CSV file.

        Raises:
            ValueError: If the ZIP URL for the date is not found.
            httpx.RequestError: If there's an issue with network requests.
            zipfile.BadZipFile: If the downloaded file is not a valid ZIP.
        """
        # Fetch the price list index page
        html_content = self.fetch_index()

        # Find the ZIP URL for the specified date
        zip_url = self.find_zip_url_for_date(html_content, date)
        if not zip_url:
            error_msg = f"No price list ZIP found for date {date}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Download and extract the ZIP file
        csv_files = self.download_and_extract_zip(zip_url)
        return csv_files

    def get_all_products(
        self, date: datetime.date
    ) -> Tuple[datetime.date, List[Store]]:
        """
        Main method to fetch and parse all products from Plodine's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            Tuple with the date and the list of Store objects,
            each containing its products.

        Raises:
            ValueError: If the price list ZIP cannot be found or processed
        """
        logger.info(f"Starting Plodine product crawl for date {date}")
        t0 = time()

        try:
            csv_files = self.get_csv_files_for_date(date)

            stores = []

            # Process each CSV file
            for filename, content in csv_files:
                try:
                    # Parse store information from the filename
                    store = self.parse_store_from_filename(filename)
                    if not store:
                        logger.warning(
                            f"Skipping CSV {filename} due to store parsing failure"
                        )
                        continue

                    # Parse CSV and add products to the store
                    products = self.parse_csv(content)
                    store.items = products
                    stores.append(store)

                except Exception as e:
                    logger.error(f"Error processing CSV {filename}: {str(e)}")
                    continue

            total_products = sum(len(store.items) for store in stores)
            t1 = time()
            log_operation_timing(
                "crawl", "Plodine", date, t0, t1, len(stores), total_products
            )
            return date, stores

        except Exception as e:
            logger.error(f"Failed to fetch or parse Plodine price list: {str(e)}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = PlodineCrawler()
    current_date = datetime.date.today()
    price_date, stores = crawler.get_all_products(current_date)
