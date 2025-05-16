import datetime
import logging
import os
import re
import tempfile
import zipfile
from decimal import Decimal
from time import time
from typing import List, Optional, Tuple

import httpx
from lxml import etree

from crawler.store.models import Store, Product
from crawler.store.utils import to_camel_case, parse_price, log_operation_timing

logger = logging.getLogger(__name__)


class StudenacCrawler:
    """
    Crawler for Studenac store prices.

    This class handles downloading and parsing price data from Studenac's website.
    It fetches the ZIP file containing XML files for each store, extracts them,
    and parses the XML data to create a structured representation of stores and their products.
    """

    BASE_URL = "https://www.studenac.hr"

    def __init__(self) -> None:
        """Initialize the Studenac crawler."""
        self.client = httpx.Client(timeout=60.0)  # Longer timeout for ZIP download

    def get_price_list_url(self, date: datetime.date) -> str:
        """
        Generate the URL for the price list ZIP file.

        Args:
            date: The date for which to get the price list

        Returns:
            URL for the price list ZIP file
        """
        date_str = date.strftime("%Y-%m-%d")
        return f"{self.BASE_URL}/cjenici/PROIZVODI-{date_str}.zip"

    def download_zip(self, url: str) -> Optional[str]:
        """
        Download the ZIP file to a temporary location.

        Args:
            url: URL of the ZIP file to download

        Returns:
            Path to the downloaded ZIP file, or None if download fails
        """
        logger.info(f"Downloading ZIP file from {url}")
        temp_path = None
        try:
            # Create a temporary file
            fd, temp_path = tempfile.mkstemp(suffix=".zip")
            os.close(fd)

            # Download the file in chunks
            with open(temp_path, "wb") as f:
                with self.client.stream("GET", url) as response:
                    response.raise_for_status()
                    total = int(response.headers.get("content-length", 0))
                    logger.debug(f"ZIP file size: {total} bytes")

                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)

            logger.debug(f"Successfully downloaded ZIP to {temp_path}")
            return temp_path
        except Exception as e:
            logger.error(f"Failed to download ZIP from {url}: {str(e)}")
            # Clean up the temporary file if it exists
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
            return None

    def parse_address(self, address: str) -> Tuple[str, str]:
        """
        Parse the address string into street address and city components.

        Args:
            address: Address string in format "<street> <number> <CITY>"

        Returns:
            Tuple of (street_address, city)
        """
        logger.debug(f"Parsing address: {address}")

        try:
            # The regex matches the last set of uppercase words (city)
            # and everything before it (street address)
            pattern = r"^(.*?)([A-ZČĆĐŠŽ][A-ZČĆĐŠŽ\s]+)$"
            match = re.match(pattern, address)

            if match:
                street_address, city = match.groups()
                return to_camel_case(street_address.strip()), to_camel_case(
                    city.strip()
                )

            # Fallback if regex doesn't match
            logger.warning(f"Failed to parse address: {address}")
            return to_camel_case(address), ""
        except Exception as e:
            logger.error(f"Error parsing address {address}: {str(e)}")
            return to_camel_case(address), ""

    def parse_xml(self, xml_content: bytes) -> Optional[Store]:
        """
        Parse XML content into a unified Store object.

        Args:
            xml_content: XML content as bytes

        Returns:
            Store object with parsed store and product information,
            or None if parsing fails
        """
        logger.debug("Parsing XML content")

        try:
            root = etree.fromstring(xml_content)

            # Extract store information
            store_type = root.xpath("//ProdajniObjekt/Oblik/text()")[0].lower()
            store_id = root.xpath("//ProdajniObjekt/Oznaka/text()")[0]
            store_code = root.xpath("//ProdajniObjekt/Oznaka/text()")[0]
            address = root.xpath("//ProdajniObjekt/Adresa/text()")[0]

            street_address, city = self.parse_address(address)

            store = Store(
                chain="studenac",
                name=f"Studenac {store_code}",
                store_type=to_camel_case(store_type),
                store_id=store_id,
                city=city,
                street_address=street_address,
                items=[],
            )

            logger.debug(
                f"Parsed store: {store.name} ({store_id}), {store.store_type}, {store.city}, {store.street_address}"
            )

            # Extract product information
            products = []
            for product_elem in root.xpath("//ProdajniObjekt/Proizvodi/Proizvod"):
                try:
                    # Helper function to get text from an XML element safely
                    def get_text(xpath, default=""):
                        elements = product_elem.xpath(xpath)
                        return elements[0] if elements and elements[0] else default

                    # Extract product fields
                    product_name = get_text("NazivProizvoda/text()")
                    product_id = get_text("SifraProizvoda/text()")
                    brand = get_text("MarkaProizvoda/text()")
                    quantity = get_text("NetoKolicina/text()")
                    unit = get_text("JedinicaMjere/text()")

                    # Parse price fields
                    price_str = get_text("MaloprodajnaCijena/text()", "0")
                    unit_price_str = get_text("CijenaPoJedinici/text()", "0")
                    special_price_str = get_text("MaloprodajnaCijenaAkcija/text()")
                    best_price_30_str = get_text("NajnizaCijena/text()", "0")
                    anchor_price_str = get_text("SidrenaCijena/text()")

                    price = parse_price(price_str)
                    unit_price = parse_price(unit_price_str)
                    special_price = (
                        parse_price(special_price_str) if special_price_str else None
                    )
                    best_price_30 = parse_price(best_price_30_str)
                    anchor_price = (
                        parse_price(anchor_price_str) if anchor_price_str else None
                    )

                    barcode = get_text("Barkod/text()")
                    category = get_text("KategorijeProizvoda/text()")

                    product = Product(
                        product=product_name,
                        product_id=product_id,
                        brand=brand,
                        quantity=quantity,
                        unit=unit,
                        price=price,
                        unit_price=unit_price,
                        special_price=special_price,
                        best_price_30=best_price_30,
                        anchor_price=anchor_price,
                        barcode=barcode,
                        category=category,
                    )

                    products.append(product)
                except Exception as e:
                    logger.warning(
                        f"Failed to parse product: {etree.tostring(product_elem)}: {str(e)}"
                    )
                    continue

            store.items = products
            logger.debug(f"Parsed {len(products)} products for store {store.name}")
            return store

        except Exception as e:
            logger.error(f"Failed to parse XML: {str(e)}")
            return None

    def process_zip_file(self, zip_path: str) -> List[Store]:
        """
        Process all XML files in the ZIP file.

        Args:
            zip_path: Path to the downloaded ZIP file

        Returns:
            List of Store objects
        """
        logger.info(f"Processing ZIP file at {zip_path}")
        stores = []

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                file_count = len(zip_ref.infolist())
                logger.debug(f"ZIP contains {file_count} files")

                for file_info in zip_ref.infolist():
                    if not file_info.filename.endswith(".xml"):
                        logger.debug(f"Skipping non-XML file: {file_info.filename}")
                        continue

                    logger.debug(f"Processing XML file: {file_info.filename}")
                    try:
                        with zip_ref.open(file_info) as file:
                            xml_content = file.read()
                            store = self.parse_xml(xml_content)
                            if store:
                                stores.append(store)
                    except Exception as e:
                        logger.error(
                            f"Error processing file {file_info.filename}: {str(e)}"
                        )

            logger.info(f"Processed {len(stores)} stores from ZIP file")
            return stores
        except Exception as e:
            logger.error(f"Failed to process ZIP file {zip_path}: {str(e)}")
            return []

    def get_all_products(
        self, date: datetime.date
    ) -> Tuple[datetime.date, List[Store]]:
        """
        Main method to fetch and parse all products from Studenac's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            Tuple with the date and the list of Store objects,
            each containing its products.

        Raises:
            ValueError: If the price list cannot be fetched or parsed
        """
        logger.info(f"Starting Studenac product crawl for date {date}")
        t0 = time()

        try:
            # Get the URL for the ZIP file
            url = self.get_price_list_url(date)

            # Download the ZIP file
            zip_path = self.download_zip(url)
            if not zip_path:
                raise ValueError(f"Failed to download ZIP file from {url}")

            # Process the ZIP file
            stores = self.process_zip_file(zip_path)
            if not stores:
                raise ValueError("Failed to extract any stores from the ZIP file")

            # Clean up
            os.remove(zip_path)

            total_products = sum(len(store.items) for store in stores)
            t1 = time()
            log_operation_timing(
                "crawl", "Studenac", date, t0, t1, len(stores), total_products
            )

            return date, stores

        except Exception as e:
            logger.error(f"Failed to fetch or parse Studenac price list: {str(e)}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = StudenacCrawler()
    current_date = datetime.date.today()
    price_date, stores = crawler.get_all_products(current_date)
