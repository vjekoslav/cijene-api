import datetime
import logging
import os
from typing import List

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class EurospinCrawler(BaseCrawler):
    """Crawler for Eurospin store prices."""

    CHAIN = "eurospin"
    BASE_URL = "https://www.eurospin.hr"
    INDEX_URL = f"{BASE_URL}/cjenik/"

    # Mapping for price fields
    PRICE_MAP = {
        # field: (column, is_required)
        "price": ("MALOPROD.CIJENA(EUR)", True),
        "unit_price": ("CIJENA_ZA_JEDINICU_MJERE", True),
        "special_price": ("MPC_POSEB.OBLIK_PROD", False),
        "best_price_30": ("NAJNIŽA_MPC_U_30DANA", False),
        "anchor_price": ("SIDRENA_CIJENA", False),
    }

    # Mapping for other fields
    FIELD_MAP = {
        "product": ("NAZIV_PROIZVODA", True),
        "product_id": ("ŠIFRA_PROIZVODA", True),
        "brand": ("MARKA_PROIZVODA", False),
        "quantity": ("NETO_KOLIČINA", False),
        "unit": ("JEDINICA_MJERE", False),
        "barcode": ("BARKOD", False),
        "category": ("KATEGORIJA_PROIZVODA", False),
    }

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the Eurospin index page to extract CSV links.

        Args:
            content: HTML content of the index page

        Returns:
            List of CSV urls on the page
        """
        soup = BeautifulSoup(content, "html.parser")
        urls = []

        csv_options = soup.select("option[value$='.csv']")
        for option in csv_options:
            href = str(option.get("value"))
            if href.startswith(("http://", "https://")):
                urls.append(href)
            else:
                urls.append(f"{self.BASE_URL}{href}")

        return list(set(urls))

    def parse_store_info(self, url: str) -> Store:
        """
        Extracts store information from a CSV download URL.

        Example URL:
        https://www.eurospin.hr/wp-content/themes/eurospin/documenti-prezzi/supermarket-310037-Ljudevita_Šestica_7-Karlovac-123456-21.05.2025-7.30.csv

        Args:
            url: CSV download URL with store information in the filename

        Returns:
            Store object with parsed store information
        """
        logger.debug(f"Parsing store information from URL: {url}")

        filename = os.path.basename(url)
        parts = filename.split("-")

        if len(parts) < 7:
            raise ValueError(f"Invalid CSV filename format: {filename}")

        store_type = parts[0].lower()
        store_id = parts[1]
        street_address = parts[2].replace("_", " ")
        city = parts[3]

        # Valid zipcode is 5 digits
        zipcode = parts[4] if len(parts[4]) == 5 and parts[4].isdigit() else ""

        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=store_id,
            name=f"{self.CHAIN.capitalize()} {city}",
            street_address=street_address,
            zipcode=zipcode,
            city=city,
            items=[],
        )

        logger.info(
            f"Parsed store: {store.store_type}, {store.street_address}, {store.zipcode}, {store.city}"
        )
        return store

    def get_store_prices(self, csv_url: str) -> List[Product]:
        """
        Fetch and parse store prices from a CSV URL.

        Args:
            csv_url: URL to the CSV file containing prices

        Returns:
            List of Product objects
        """
        try:
            content = self.fetch_text(csv_url)
            return self.parse_csv(content, delimiter=";")
        except Exception as e:
            logger.error(
                f"Failed to get store prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def get_index(self, date: datetime.date) -> list[str]:
        """
        Fetch and parse the index page to get CSV URLs for the specified date.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of CSV URLs containing prices for the specified date
        """
        content = self.fetch_text(self.INDEX_URL)

        if not content:
            logger.warning(f"No content found at {self.INDEX_URL}")
            return []

        all_urls = self.parse_index(content)
        date_str = f"{date.day:02d}.{date.month:02d}.{date.year}"

        # Filter URLs by date
        matching_urls = []
        for url in all_urls:
            filename = os.path.basename(url)
            if date_str in filename:
                matching_urls.append(url)

        if not matching_urls:
            logger.warning(f"No URLs found matching date {date_str}")

        return matching_urls

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all store, product and price info.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.

        Raises:
            ValueError: If no price list is found for the given date.
        """
        csv_links = self.get_index(date)

        if not csv_links:
            logger.warning(f"No CSV links found for date {date}")
            return []

        stores = []

        for url in csv_links:
            try:
                store = self.parse_store_info(url)
                products = self.get_store_prices(url)
            except Exception as e:
                logger.error(f"Error processing store from {url}: {e}", exc_info=True)
                continue

            if not products:
                logger.warning(f"No products found for store at {url}, skipping")
                continue

            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = EurospinCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
