import csv
import datetime
import logging
import tempfile
import zipfile
from decimal import Decimal
from io import StringIO
from time import time
from typing import List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup

from crawler.store.models import Store, Product
from crawler.store.utils import parse_price, log_operation_timing, to_camel_case

logger = logging.getLogger(__name__)


class LidlCrawler:
    """
    Crawler for Lidl store prices.

    This class handles downloading and parsing price data from Lidl's website.
    It fetches the price list index page, finds the ZIP for the specified date,
    downloads and extracts it, and parses the CSV files inside.
    """

    BASE_URL = "https://tvrtka.lidl.hr"
    PRICE_LIST_URL = f"{BASE_URL}/cijene"

    def __init__(self) -> None:
        """Initialize the Lidl crawler."""
        self.client = httpx.Client(timeout=180.0)  # Longer timeout for ZIP download

    def fetch_index(self) -> str:
        """
        Fetches the price list index page from Lidl's website.

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

        # Format date for comparison (DD_MM_YYYY)
        target_date_str = target_date.strftime("%d_%m_%Y")

        # Find all links on the page
        links = soup.find_all("a", href=True)

        for link in links:
            # Check if the link contains the target date in filename format
            href = link["href"]
            if ".zip" in href and target_date_str in href:
                # Make absolute URL if needed
                zip_url = (
                    href
                    if href.startswith("http")
                    else f"{self.BASE_URL}/{href.lstrip('/')}"
                )
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
                                # Decode from Windows-1250 (encoding used by Lidl)
                                content = f.read().decode("windows-1250")
                                csv_files.append((filename, content))
                                logger.debug(
                                    f"Extracted CSV: {filename}, size: {len(content)} bytes"
                                )
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
        Extract store information from CSV filename using filename parts.

        Args:
            filename: Name of the CSV file with store information

        Returns:
            Store object with parsed store information, or None if parsing fails
        """
        logger.debug(f"Parsing store information from filename: {filename}")

        try:
            # Examples: "Supermarket 104_Jastrebarsko_Dr. F. Tudmana 30_10450_Jastrebarsko_16.05.2025_7.15h.csv"
            # First, split by underscore
            parts = filename.split("_")

            # Handle case when we don't have enough parts
            if len(parts) < 5:
                logger.warning(f"Not enough parts in filename: {filename}")
                return None

            # The first part should start with "Supermarket"
            first_part = parts[0]
            if not first_part.startswith("Supermarket"):
                logger.warning(f"Filename doesn't start with 'Supermarket': {filename}")
                return None

            # The city is the second element
            city = to_camel_case(parts[1])

            # Street address is the third element
            street_address = to_camel_case(parts[2])

            # Zipcode is the fourth element
            zipcode = parts[3]

            # Format the store information
            store_name = f"Lidl {city}"
            store_type = "supermarket"
            store_id = first_part.replace("Supermarket", "").strip()

            store = Store(
                chain="lidl",
                store_id=store_id,
                name=store_name,
                store_type=store_type,
                city=city,
                street_address=street_address,
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

    def parse_csv(self, csv_content: str) -> List[Product]:
        """
        Parses CSV content into unified Product objects.

        Args:
            csv_content: CSV content as a string

        Returns:
            List of Product objects
        """
        logger.debug("Parsing CSV content")

        products = []
        reader = csv.DictReader(StringIO(csv_content), delimiter=",")

        for row in reader:
            try:
                anchor_price = row.get("Sidrena_cijena_na_02.05.2025", "").strip()
                if "Nije_bilo_u_prodaji" in anchor_price:
                    anchor_price = None

                product = Product(
                    product=row.get("NAZIV", ""),
                    product_id=row.get("ŠIFRA", ""),
                    brand=row.get("MARKA", ""),
                    quantity=row.get("NETO_KOLIČINA", ""),
                    unit=row.get("JEDINICA_MJERE", ""),
                    packaging=row.get("PAKIRANJE", None),
                    price=parse_price(row.get("MALOPRODAJNA_CIJENA", "")),
                    unit_price=parse_price(row.get("CIJENA_ZA_JEDINICU_MJERE", "")),
                    barcode=row.get("BARKOD", ""),
                    category=row.get("KATEGORIJA_PROIZVODA", ""),
                    anchor_price=parse_price(anchor_price) if anchor_price else None,
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
        html_content = self.fetch_index()

        zip_url = self.find_zip_url_for_date(html_content, date)
        if not zip_url:
            error_msg = f"No price list ZIP found for date {date}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        csv_files = self.download_and_extract_zip(zip_url)
        return csv_files

    def get_all_products(
        self, date: datetime.date
    ) -> Tuple[datetime.date, List[Store]]:
        """
        Main method to fetch and parse all products from Lidl's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            Tuple with the date and the list of Store objects,
            each containing its products.

        Raises:
            ValueError: If the price list ZIP cannot be found or processed
        """
        logger.info(f"Starting Lidl product crawl for date {date}")
        t0 = time()

        try:
            csv_files = self.get_csv_files_for_date(date)

            stores = []

            for filename, content in csv_files:
                try:
                    store = self.parse_store_from_filename(filename)
                    if not store:
                        logger.warning(
                            f"Skipping CSV {filename} due to store parsing failure"
                        )
                        continue

                    products = self.parse_csv(content)
                    store.items = products
                    stores.append(store)

                except Exception as e:
                    logger.error(f"Error processing CSV {filename}: {str(e)}")
                    continue

            total_products = sum(len(store.items) for store in stores)
            t1 = time()
            log_operation_timing(
                "crawl", "Lidl", date, t0, t1, len(stores), total_products
            )
            return date, stores

        except Exception as e:
            logger.error(f"Failed to fetch or parse Lidl price list: {str(e)}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    crawler = LidlCrawler()
    current_date = datetime.date.today()
    price_date, stores = crawler.get_all_products(current_date)
