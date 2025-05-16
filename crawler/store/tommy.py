import csv
import datetime
import io
import logging
import re
from decimal import Decimal
from time import time
from typing import Dict, List, Optional, Tuple

import httpx

from crawler.store.models import Product, Store
from crawler.store.utils import parse_price, to_camel_case, log_operation_timing

logger = logging.getLogger(__name__)


class TommyCrawler:
    """
    Crawler for Tommy store prices.

    This class handles downloading and parsing price data from Tommy's API.
    It retrieves JSON data about available store price tables and processes
    the corresponding CSV files for product information.
    """

    BASE_URL = "https://spiza.tommy.hr/api/v2"  # Note: Already includes api/v2

    def __init__(self) -> None:
        """Initialize the Tommy crawler."""
        self.client = httpx.Client(timeout=30.0)

    def fetch_stores_list(self, date: datetime.date) -> List[Dict]:
        """
        Fetch the list of store price tables for a specific date.

        Args:
            date: The date for which to fetch the price tables

        Returns:
            List of dictionaries containing store price table information

        Raises:
            httpx.RequestError: If the API request fails
            ValueError: If the response cannot be parsed
        """
        formatted_date = date.strftime("%Y-%m-%d")
        url = f"{self.BASE_URL}/shop/store-prices-tables?date={formatted_date}&page=1&itemsPerPage=200&channelCode=general"

        logger.info(f"Fetching Tommy store list for date {formatted_date}")

        try:
            response = self.client.get(url)
            response.raise_for_status()

            data = response.json()
            store_list = data.get("hydra:member", [])
            logger.info(f"Found {len(store_list)} Tommy stores in the API response")

            if not store_list:
                logger.warning(f"No Tommy stores found for date {formatted_date}")

            return store_list

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error fetching Tommy store list: {e.response.status_code} - {e}"
            )
            raise ValueError(
                f"Failed to fetch Tommy store list: HTTP {e.response.status_code}"
            )

        except httpx.RequestError as e:
            logger.error(f"Request error fetching Tommy store list: {e}")
            raise ValueError("Failed to fetch Tommy store list: Connection error")

        except ValueError as e:
            logger.error(f"JSON parsing error in Tommy store list response: {e}")
            raise ValueError(f"Failed to parse Tommy store list response: {e}")

    def download_csv(self, csv_id: str) -> str:
        """
        Download the CSV file for a specific store.

        Args:
            store_id: The ID of the store from the API (@id value)

        Returns:
            Content of the CSV file

        Raises:
            ValueError: If the download fails or yields empty content
        """
        try:
            # The @id from API looks like: "/api/v2/shop/store-prices-tables/SUPERMARKET,..."
            # But our BASE_URL already contains "https://spiza.tommy.hr/api/v2"
            # So we need to be careful to avoid duplication

            logger.debug(f"Original store_id: {csv_id}")

            # Extract everything after "/api/v2" if it exists
            if "/api/v2/" not in csv_id:
                raise ValueError(f"Unexpected store id: {csv_id}")

            path = csv_id.split("/api/v2/", 1)[1]
            download_url = f"{self.BASE_URL}/{path}"

            logger.info(f"Downloading Tommy CSV from {download_url}")
            response = self.client.get(
                download_url, timeout=60.0
            )  # Longer timeout for CSV download
            response.raise_for_status()

            content = response.text
            content_size = len(content)

            if content_size == 0:
                raise ValueError("Downloaded CSV is empty")

            logger.debug(f"CSV downloaded successfully, size: {content_size} bytes")
            return content

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error downloading CSV: {e.response.status_code} - {e}")
            raise ValueError(f"Failed to download CSV: HTTP {e.response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Request error downloading CSV: {e}")
            raise ValueError("Failed to download CSV: Connection error")

        except Exception as e:
            logger.error(f"Unexpected error downloading CSV: {e}")
            raise ValueError(f"Failed to download CSV: {str(e)}")

    def parse_date_string(self, date_str: str) -> Optional[datetime.date]:
        """
        Parse date string from CSV (format DD.MM.YYYY. HH:MM:SS).

        Args:
            date_str: The date string to parse (e.g., "16.5.2025. 0:00:00")

        Returns:
            datetime.date object or None if parsing fails
        """
        if not date_str or date_str.strip() == "":
            return None

        try:
            # Use regex to extract day, month, and year
            # The pattern handles both single and double-digit day/month
            match = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})\.", date_str)

            if match:
                day, month, year = map(int, match.groups())
                return datetime.date(year, month, day)
            else:
                logger.warning(f"Date string format not recognized: {date_str}")
                return None

        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse date string '{date_str}': {e}")
            return None

    def parse_csv(self, csv_content: str) -> List[Product]:
        """
        Parse CSV content and extract product information.

        Args:
            csv_content: Content of the CSV file

        Returns:
            List of Product objects

        CSV format:
            BARKOD_ARTIKLA,SIFRA_ARTIKLA,NAZIV_ARTIKLA,BRAND,ROBNA_STRUKTURA,
            JEDINICA_MJERE,NETO_KOLICINA,MPC,MPC_POSEBNA_PRODAJA,CIJENA_PO_JM,
            MPC_NAJNIZA_30,MPC_020525,DATUM_ULASKA_NOVOG_ARTIKLA,PRVA_CIJENA_NOVOG_ARTIKLA
        """
        products = []
        row_count = 0
        success_count = 0
        error_count = 0

        try:
            # Read CSV content using StringIO
            csv_file = io.StringIO(csv_content)
            reader = csv.reader(csv_file)

            # Skip header row
            header = next(reader, None)
            if not header:
                logger.warning("CSV file has no header row")
                return products

            logger.debug(f"CSV header: {header}")

            # Process each row
            for row in reader:
                row_count += 1

                if len(row) < 11:
                    logger.warning(
                        f"Skipping invalid row {row_count} with insufficient columns: {row}"
                    )
                    error_count += 1
                    continue

                try:
                    # Extract mandatory fields from the row
                    barcode = row[0].strip()
                    product_id = row[1].strip()
                    product_name = row[2].strip()
                    brand = row[3].strip()
                    category = row[4].strip()
                    unit = row[5].strip()
                    quantity = row[6].strip()

                    # Parse price fields with proper error handling
                    try:
                        price = parse_price(row[7])
                    except Exception as e:
                        logger.warning(f"Failed to parse price in row {row_count}: {e}")
                        price = Decimal("0.00")

                    try:
                        unit_price = parse_price(row[9])
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse unit_price in row {row_count}: {e}"
                        )
                        unit_price = Decimal("0.00")

                    # Parse optional price fields
                    special_price = None
                    lowest_price_30days = None
                    anchor_price = None
                    initial_price = None
                    date_added = None

                    if len(row) > 8 and row[8].strip():
                        try:
                            special_price = parse_price(row[8])
                        except Exception:
                            pass

                    if len(row) > 10 and row[10].strip():
                        try:
                            lowest_price_30days = parse_price(row[10])
                        except Exception:
                            pass

                    if len(row) > 11 and row[11].strip():
                        try:
                            anchor_price = parse_price(row[11])
                        except Exception:
                            pass

                    if len(row) > 12 and row[12].strip():
                        date_added = self.parse_date_string(row[12])

                    if len(row) > 13 and row[13].strip():
                        try:
                            initial_price = parse_price(row[13])
                        except Exception:
                            pass

                    # Create product if we have the minimum required fields
                    if product_name and (price or unit_price):
                        # If one price is missing but the other exists, use the existing one for both
                        if price and not unit_price:
                            unit_price = price
                        elif unit_price and not price:
                            price = unit_price

                        product = Product(
                            product=product_name,
                            product_id=product_id,
                            barcode=barcode,
                            brand=brand,
                            category=category,
                            unit=unit,
                            quantity=quantity,
                            price=price,
                            special_price=special_price,
                            unit_price=unit_price,
                            best_price_30=lowest_price_30days, # Map lowest_price_30days to best_price_30
                            anchor_price=anchor_price,
                            date_added=date_added,
                            initial_price=initial_price,
                        )
                        products.append(product)
                        success_count += 1
                    else:
                        logger.warning(
                            f"Skipping product in row {row_count} with missing required fields"
                        )
                        error_count += 1

                except Exception as e:
                    logger.error(f"Error parsing product row {row_count}: {e}")
                    logger.debug(f"Problematic row: {row}")
                    error_count += 1

            logger.info(
                f"Parsed {len(products)} products from CSV (total rows: {row_count}, errors: {error_count})"
            )
            return products

        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            return []

    def parse_store_from_filename(self, filename: str) -> Tuple[str, str, str, str, str]:
        """
        Parse store information from the filename.

        Args:
            filename: The filename from the API

        Returns:
            Tuple of (store_type, address, zipcode, city)

        Example:
            "SUPERMARKET, ANTE STARČEVIĆA 6, 20260 KORČULA, 10180, 2, 20250516 0530"
            Will return:
            ("supermarket", "10180", "Ante Starčevića 6", "20260", "Korčula")
        """
        try:
            # Split by commas
            parts = filename.split(",")

            if len(parts) < 3:
                logger.warning(f"Filename doesn't have enough parts: {filename}")
                raise ValueError(f"Unparseable filename: {filename}")

            # Extract store type (first part)
            store_type = parts[0].strip().lower()

            # Extract address (second part)
            address = to_camel_case(parts[1].strip())

            # Extract zipcode and city (third part)
            location_part = parts[2].strip()

            # Use regex to extract zipcode and city
            # Pattern looks for 5 digits followed by any text
            match = re.match(r"(\d{5})\s+(.+)", location_part)

            if match:
                zipcode = match.group(1)
                city = to_camel_case(match.group(2))
            else:
                logger.warning(
                    f"Could not extract zipcode and city from: {location_part}"
                )
                zipcode = ""
                # Try to extract just the city if no zipcode pattern found
                city = to_camel_case(location_part)

            store_id = parts[3]

            logger.debug(
                f"Parsed store info: type={store_type}, address={address}, zipcode={zipcode}, city={city}"
            )



            return (store_type, store_id, address, zipcode, city)

        except Exception as e:
            logger.error(f"Error parsing store from filename {filename}: {e}")
            raise

    def get_all_products(
        self, date: datetime.date
    ) -> Tuple[datetime.date, List[Store]]:
        """
        Main method to fetch and parse all products from Tommy's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            Tuple with the date and the list of Store objects,
            each containing its products.

        Raises:
            ValueError: If the price list cannot be fetched or parsed
        """
        logger.info(f"Starting Tommy product crawl for date {date}")
        t0 = time()

        try:
            # Fetch store list
            stores_list = self.fetch_stores_list(date)

            if not stores_list:
                logger.warning(f"No stores found for date {date}")
                return date, []

            result_stores: List[Store] = []
            total_products = 0
            processed_count = 0
            error_count = 0

            # Process each store
            for store_info in stores_list:
                processed_count += 1
                csv_id = store_info.get("@id")
                filename = store_info.get("fileName", "Unknown")

                if not csv_id:
                    logger.warning(f"Skipping store with missing CSV ID: {store_info}")
                    error_count += 1
                    continue

                try:
                    logger.info(
                        f"Processing store {processed_count}/{len(stores_list)}: {filename}"
                    )

                    # Extract store information
                    store_type, store_id, address, zipcode, city = self.parse_store_from_filename(
                        filename
                    )

                    # Create store
                    store = Store(
                        chain="tommy",
                        name=f"Tommy {store_type.title()} {address}",
                        store_type=store_type,
                        store_id=store_id,
                        city=city,
                        street_address=address,
                        zipcode=zipcode,
                        items=[],
                    )

                    # Download and parse CSV
                    csv_content = self.download_csv(csv_id)
                    products = self.parse_csv(csv_content)

                    # Skip stores with no products
                    if not products:
                        logger.warning(f"Skipping store with no products: {store.name}")
                        error_count += 1
                        continue

                    # Add products to store
                    store.items = products
                    result_stores.append(store)
                    total_products += len(products)

                    logger.info(
                        f"Successfully processed store: {store.name}, found {len(products)} products"
                    )

                except Exception as e:
                    logger.error(f"Error processing store {filename}: {e}")
                    error_count += 1

            t1 = time()
            log_operation_timing(
                "crawl", "Tommy", date, t0, t1, len(result_stores), total_products
            )

            # Log summary
            logger.info(
                f"Tommy crawl summary: processed {processed_count} stores, {error_count} errors, {len(result_stores)} successful"
            )

            return date, result_stores

        except Exception as e:
            t1 = time()
            dt = int(t1 - t0)
            logger.error(
                f"Failed to crawl Tommy products for {date}: {e} (after {dt}s)"
            )
            raise ValueError(f"Failed to crawl Tommy products: {str(e)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = TommyCrawler()
    current_date = datetime.date.today()
    price_date, stores = crawler.get_all_products(current_date)
