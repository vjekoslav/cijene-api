import csv
import datetime
from time import time
import logging
import urllib.parse
from io import StringIO
from typing import List, Optional, Tuple, Any

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

from crawler.store.utils import to_camel_case, log_operation_timing, parse_price

logger = logging.getLogger(__name__)


class KonzumProduct(BaseModel):
    """
    Represents a product from Konzum's price list.
    """

    product: str
    product_id: str
    brand: str
    quantity: str
    unit: str
    price: float
    unit_price: float
    barcode: str
    category: str
    special_price: Optional[float] = None  # "MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE"
    best_price_30: Optional[float] = None  # "NAJNIŽA CIJENA U POSLJEDNJIH 30 DANA"
    anchor_price: Optional[float] = None  # "SIDRENA CIJENA NA 2.5.2025"


class KonzumStore(BaseModel):
    """
    Represents a Konzum store with its products.
    """

    store_type: str
    street_address: str
    zipcode: str
    city: str
    items: List[KonzumProduct] = Field(default_factory=list)


class KonzumCrawler:
    """
    Crawler for Konzum store prices.

    This class handles downloading and parsing price data from Konzum's website.
    It fetches the price list page, extracts CSV links, downloads and parses
    the CSVs, and returns a list of products.
    """

    BASE_URL = "https://www.konzum.hr"
    PRICE_LIST_URL = f"{BASE_URL}/cjenici"

    def __init__(self) -> None:
        """Initialize the Konzum crawler."""
        self.client = httpx.Client(timeout=30.0)

    def fetch_index(self) -> str:
        """
        Fetches the price list page from Konzum's website.

        Returns:
            str: HTML content of the price list page

        Raises:
            httpx.RequestError: If the request fails
        """
        logger.info(f"Fetching price list page from {self.PRICE_LIST_URL}")
        response = self.client.get(self.PRICE_LIST_URL)
        response.raise_for_status()
        logger.debug(
            f"Successfully fetched price list page, size: {len(response.text)} bytes"
        )
        return response.text

    def extract_price_date(
        self, soup: BeautifulSoup, target_date: datetime.date
    ) -> Tuple[str, Optional[Any]]:
        """
        Extracts the price date div from the BeautifulSoup object that matches the target date.

        Args:
            soup: BeautifulSoup object of the price list page
            target_date: The date to search for in the price list

        Returns:
            Tuple containing the date string and the date div element

        Raises:
            ValueError: If the date div matching the target date cannot be found
        """
        logger.debug(f"Extracting price date from page matching {target_date}")

        # Find all divs with data-tab-type attribute
        date_divs = soup.find_all("div", attrs={"data-tab-type": True})

        if not date_divs:
            error_msg = "No date divs found on the page"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Format target date to match expected format (YYYYMMDD)
        target_date_str = target_date.strftime("%Y%m%d")

        # Find the div that matches the target date
        matching_div = None
        date_value = None

        for div in date_divs:
            div_date = div.get("data-tab-type")
            if div_date == target_date_str:
                matching_div = div
                date_value = div_date
                break

        if not matching_div:
            available_dates = [
                div.get("data-tab-type")
                for div in date_divs
                if div.get("data-tab-type")
            ]
            error_msg = f"No price list found for date {target_date_str}. Available dates: {', '.join(available_dates)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not date_value:
            error_msg = "Date value is empty"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(f"Found price date: {date_value}")
        return date_value, matching_div

    def find_csv_links(self, date_div: Any) -> List[str]:
        """
        Finds all CSV download links within the date div.

        Args:
            date_div: BeautifulSoup element containing the CSV links

        Returns:
            List of CSV download URLs
        """
        logger.debug("Finding CSV links in date div")

        links = []
        csv_links = date_div.find_all("a", attrs={"format": "csv"})

        for link in csv_links:
            href = link.get("href")
            if href:
                full_url = f"{self.BASE_URL}{href}"
                links.append(full_url)

        logger.info(f"Found {len(links)} CSV links")
        logger.debug(f"CSV links: {links}")
        return links

    def download_csv(self, url: str) -> Optional[str]:
        """
        Downloads a CSV file from the given URL.

        Args:
            url: URL of the CSV file to download

        Returns:
            CSV content as a string, or None if download fails
        """
        logger.debug(f"Downloading CSV from {url}")

        try:
            response = self.client.get(url)
            response.raise_for_status()
            logger.debug(
                f"Successfully downloaded CSV, size: {len(response.text)} bytes"
            )
            return response.text
        except httpx.RequestError as e:
            logger.error(f"Failed to download CSV from {url}: {str(e)}")
            return None

    def parse_csv(self, csv_content: str) -> List[KonzumProduct]:
        """
        Parses CSV content into KonzumProduct objects.

        Args:
            csv_content: CSV content as a string

        Returns:
            List of KonzumProduct objects
        """
        logger.debug("Parsing CSV content")

        products = []
        reader = csv.DictReader(StringIO(csv_content), delimiter=",")

        for row in reader:
            try:
                # Convert potential empty strings to 0.0 for numeric fields
                maloprodajna_cijena = parse_price(row.get("MALOPRODAJNA CIJENA", "0"))
                cijena_za_jedinicu = parse_price(
                    row.get("CIJENA ZA JEDINICU MJERE", "0")
                )

                # Parse the new price fields, handling potential empty values
                special_price_str = row.get(
                    "MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE", ""
                )
                best_price_30_str = row.get("NAJNIŽA CIJENA U POSLJEDNJIH 30 DANA", "")
                anchor_price_str = row.get("SIDRENA CIJENA NA 2.5.2025", "")

                # Convert to float or None if empty
                # Parse price fields if present
                special_price = (
                    parse_price(special_price_str) if special_price_str else None
                )
                best_price_30 = (
                    parse_price(best_price_30_str) if best_price_30_str else 0.0
                )
                anchor_price = (
                    parse_price(anchor_price_str) if anchor_price_str else None
                )

                product = KonzumProduct(
                    product=row.get("NAZIV PROIZVODA", ""),
                    product_id=row.get("ŠIFRA PROIZVODA", ""),
                    brand=row.get("MARKA PROIZVODA", ""),
                    quantity=row.get("NETO KOLIČINA", ""),
                    unit=row.get("JEDINICA MJERE", ""),
                    price=maloprodajna_cijena,
                    unit_price=cijena_za_jedinicu,
                    barcode=row.get("BARKOD", ""),
                    category=row.get("KATEGORIJA PROIZVODA", ""),
                    special_price=special_price,
                    best_price_30=best_price_30,
                    anchor_price=anchor_price,
                )
                products.append(product)
            except Exception as e:
                logger.warning(f"Failed to parse row: {row}. Error: {str(e)}")
                continue

        logger.debug(f"Parsed {len(products)} products from CSV")
        return products

    def get_index(self, target_date: datetime.date) -> Tuple[datetime.date, List[str]]:
        """
        Fetches and parses the price list index page for the specified date.

        Args:
            target_date: The date to search for in the price list

        Returns:
            Tuple containing:
                - date: The price date as a datetime.date object
                - urls: List of CSV download URLs

        Raises:
            ValueError: If the index page cannot be fetched or parsed or if no price list
                      is found for the target date
        """
        logger.info(f"Parsing Konzum price list index page for date: {target_date}")

        # Fetch and parse the price list page
        html_content = self.fetch_index()
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract the price date and div for the target date
        date_value, date_div = self.extract_price_date(soup, target_date)

        # Convert string date to datetime.date object
        date_obj = datetime.datetime.strptime(date_value, "%Y%m%d").date()

        # Find CSV links
        csv_links = self.find_csv_links(date_div)

        logger.info(
            f"Parsed index page: date={date_obj}, found {len(csv_links)} CSV links"
        )
        return date_obj, csv_links

    def parse_store_from_url(self, url: str) -> Optional[KonzumStore]:
        """
        Extracts store information from a CSV download URL.

        Args:
            url: CSV download URL with store information in the query parameters

        Returns:
            KonzumStore object with parsed store information, or None if parsing fails
        """
        logger.debug(f"Parsing store information from URL: {url}")

        try:
            # Parse the URL to extract query parameters
            parsed_url = urllib.parse.urlparse(url)
            query_params = urllib.parse.parse_qs(parsed_url.query)

            # Get the 'title' parameter and decode it
            if "title" not in query_params or not query_params["title"]:
                logger.warning(f"No title parameter found in URL: {url}")
                return None

            title = urllib.parse.unquote(query_params["title"][0])
            logger.debug(f"Decoded title: {title}")

            # Check if this is the new format (contains spaces)
            if " " in title:
                # New format: HIPERMARKET,TRG HRVATSKIH REDARSTVENIKA 1 47000 KARLOVAC,1300,454,20250516080228.CSV
                parts = title.split(",")
                if len(parts) < 2:
                    logger.warning(f"Invalid title format, insufficient parts: {title}")
                    return None

                # Extract store type
                store_type = to_camel_case(parts[0])

                # Extract address and use regex to identify zipcode and city
                address_string = parts[1]
                import re

                # Look for 4+ digit sequence which is the zipcode
                zipcode_match = re.search(r"\b\d{4,}\b", address_string)

                if zipcode_match:
                    zipcode = zipcode_match.group(0)
                    # Split address at the zipcode
                    address_parts = address_string.split(zipcode)

                    if len(address_parts) >= 2:
                        # First part is street address (trim trailing spaces)
                        street_address = to_camel_case(address_parts[0].strip())
                        # Second part is city (trim leading spaces)
                        city = to_camel_case(address_parts[1].strip())
                    else:
                        logger.warning(
                            f"Could not parse address properly: {address_string}"
                        )
                        street_address = to_camel_case(address_string)
                        city = ""
                else:
                    logger.warning(
                        f"Could not find zipcode in address: {address_string}"
                    )
                    street_address = to_camel_case(address_string)
                    zipcode = ""
                    city = ""
            else:
                # Old format: Split by comma to get components
                parts = title.split(",")
                if len(parts) < 2:
                    logger.warning(f"Invalid title format, insufficient parts: {title}")
                    return None

                # Extract store type and address
                store_type = to_camel_case(parts[0])
                address_str = parts[1]

                # Parse address components
                address_parts = address_str.split("_")
                if len(address_parts) < 3:
                    logger.warning(f"Invalid address format: {address_str}")
                    return None

                street_address = to_camel_case(address_parts[0])
                zipcode = address_parts[1]
                city = to_camel_case("_".join(address_parts[2:]))

            store = KonzumStore(
                store_type=store_type,
                street_address=street_address,
                zipcode=zipcode,
                city=city,
                items=[],
            )

            logger.info(
                f"Parsed store: {store.store_type}, {store.street_address}, {store.zipcode}, {store.city}"
            )
            return store

        except Exception as e:
            logger.error(f"Failed to parse store from URL {url}: {str(e)}")
            return None

    def get_all_products(
        self,
        target_date: datetime.date = datetime.date.today(),
    ) -> Tuple[datetime.date, List[KonzumStore]]:
        """
        Main method to fetch and parse all products from Konzum's price lists.

        Args:
            target_date: The date to search for in the price list.
                        If None, uses the current date.

        Returns:
            Tuple with the date and the list of KonzumStore objects,
            each containing its products.

        Raises:
            ValueError: If the price list page cannot be fetched or parsed
                      or if no price list is found for the target date
        """
        if target_date is None:
            target_date = datetime.date.today()

        logger.info(f"Starting Konzum product crawl for date: {target_date}")
        t0 = time()

        try:
            # Parse the index page to get date and CSV links
            price_date, csv_links = self.get_index(target_date)

            stores = []

            # Process each CSV link
            for url in csv_links:
                try:
                    # Parse store information from the URL
                    store = self.parse_store_from_url(url)
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
                    break

                except Exception as e:
                    logger.error(f"Error processing CSV from {url}: {str(e)}")
                    continue

            total_products = sum(len(store.items) for store in stores)
            t1 = time()
            log_operation_timing(
                "crawl", "Konzum", price_date, t0, t1, len(stores), total_products
            )
            return price_date, stores

        except Exception as e:
            logger.error(f"Failed to fetch or parse Konzum price list: {str(e)}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = KonzumCrawler()
    current_date = datetime.date.today()
    price_date, stores = crawler.get_all_products(current_date)
