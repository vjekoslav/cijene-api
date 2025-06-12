import datetime
import logging
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from crawler.store.base import BaseCrawler
from crawler.store.models import Product, Store

logger = logging.getLogger(__name__)


class RotoCrawler(BaseCrawler):
    """
    Crawler for Roto store prices.
    https://www.rotodinamic.hr/cjenici/
    """

    CHAIN = "roto"
    BASE_URL = "https://www.rotodinamic.hr"
    INDEX_URL = f"{BASE_URL}/cjenici/"

    ANCHOR_PRICE_COLUMN = "sidrena cijena na 2.5.2025."
    PRICE_MAP = {
        "price": ("MPC", True),
        "unit_price": ("Cijena za jedinicu mjere", True),
        "special_price": ("MPC za vrijeme posebnog oblika prodaje", False),
        "best_price_30": ("Najniža cijena u posljednjih 30 dana", False),
        "anchor_price": (ANCHOR_PRICE_COLUMN, False),
    }

    FIELD_MAP = {
        "product": ("Naziv artikla", False),
        "product_id": ("Šifra artikla", True),
        "brand": ("BRAND", False),
        "quantity": ("neto koli?ina", False),
        "unit": ("Jedinica mjere", False),
        "barcode": ("Barkod", False),
        "category": ("Kategorija proizvoda", False),
        "packaging": ("PAKIRANJE", False),
    }

    def get_csv_url(self, date: datetime.date) -> str:
        html_content = self.fetch_text(self.INDEX_URL)
        soup = BeautifulSoup(html_content, "html.parser")
        anchors = soup.select("a.cjenici-table-row")
        hr_date = date.strftime("%d.%m.%Y")

        for anchor in anchors:
            url = anchor.attrs["href"]
            assert isinstance(url, str)
            url_date = urlparse(url).path.split(",")[-2].strip()
            if url_date == hr_date:
                return url

        raise ValueError(f"No price list found for {date}")

    def get_all_products(self, date: datetime.date) -> list[Store]:
        csv_url = self.get_csv_url(date)
        # Roto has the same prices for all stores
        products = self.get_store_products(csv_url)
        return list(self.get_stores_from_url(csv_url, products))

    def get_store_products(self, csv_url: str) -> list[Product]:
        try:
            content = self.fetch_text(csv_url, encodings=["cp1250"])
            return self.parse_csv(content, delimiter=";")
        except Exception:
            logger.exception(f"Failed to get store prices from {csv_url}")
            return []

    def get_stores_from_url(self, csv_url: str, products: list[Product]):
        matches = re.findall(r",\s*(D\d+) ([^,]+),", csv_url)
        for store_id, city in matches:
            yield Store(
                chain=self.CHAIN,
                store_type="Cash & Carry",
                store_id=store_id,
                name="",
                street_address="",
                zipcode="",
                city=city,
                items=products,
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    crawler = RotoCrawler()
    stores = crawler.get_all_products(datetime.date(2025, 6, 10))
    from pprint import pp

    pp(stores[0])
