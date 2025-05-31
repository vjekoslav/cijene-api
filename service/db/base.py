from abc import ABC, abstractmethod
from datetime import date
from typing import Dict, Any, Optional

from .models import Chain, Store, ChainProduct, Price


class DatabaseInterface(ABC):
    """Base abstract class for database implementations."""

    @abstractmethod
    async def connect(self) -> None:
        """Initialize the database connection."""
        pass

    @abstractmethod
    async def create_tables(self) -> None:
        """Create all necessary tables and indices if they don't exist."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close all database connections."""
        pass

    # Chain methods
    @abstractmethod
    async def add_chain(self, chain: Chain) -> int:
        """
        Add a new chain or get existing one and return its ID.

        Args:
            chain: Chain object containing code.

        Returns:
            The database ID of the created or existing chain.
        """
        pass

    @abstractmethod
    async def add_store(self, store: Store) -> int:
        """
        Add a new store or update existing one and return its ID.

        Args:
            store: Store object containing chain_id, code, type,
                address, city, and zipcode.

        Returns:
            The database ID of the created or updated store.
        """
        pass

    @abstractmethod
    async def get_product_barcodes(self) -> dict[str, int]:
        """
        Get all product barcodes (EANs).

        Returns:
            A dictionary mapping EANs to product IDs.
        """
        pass

    @abstractmethod
    async def get_chain_product_map(self, chain_id: int) -> dict[str, int]:
        """
        Get a mapping from chain product codes to database IDs.

        Args:
            chain_id: The ID of the chain to fetch products for.

        Returns:
            A dictionary mapping product codes to product IDs in the database.
        """

    @abstractmethod
    async def add_ean(self, ean: str) -> int:
        """
        Add empty product with only EAN.

        Args:
            ean: The EAN code to add.

        Returns:
            The ID of the created product.
        """
        pass

    @abstractmethod
    async def add_many_prices(self, prices: list[Price]) -> int:
        """
        Add multiple prices in a batch operation.

        Prices that already exist will be skipped without update.

        Args:
            prices: List of Price objects to add.

        Returns:
            The number of prices newly added.
        """
        pass

    @abstractmethod
    async def add_many_chain_products(
        self,
        chain_products: list[ChainProduct],
    ) -> int:
        """
        Add multiple chain products in a batch operation.

        Chain products that already exist will be skipped without
        update.

        Args:
            chain_products: List of ChainProduct objects to add.

        Returns:
            The number of chain products newly added.
        """
        pass

    @abstractmethod
    async def compute_chain_prices(self, date: date) -> None:
        """
        Compute chain prices for a specific date.

        This method computes min/avg/max prices for all products in all stores
        for a given chain and date, and stores them in a separate table.

        Args:
            date: The date for which to compute prices.
        """
        pass

    @abstractmethod
    async def get_user_by_api_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        """
        Get active user by API key.

        Args:
            api_key: The API key to search for.

        Returns:
            A dictionary with user information if found, otherwise None.
        """
        pass
