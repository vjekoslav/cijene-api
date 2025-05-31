from contextlib import asynccontextmanager
import asyncpg
from typing import (
    AsyncGenerator,
    AsyncIterator,
    List,
    Dict,
    Any,
    Optional,
)
import logging
import os
from datetime import date
from .base import Database
from .models import Chain, Store, ChainProduct, Price


class PostgresDatabase(Database):
    """PostgreSQL implementation of the database interface using asyncpg."""

    def __init__(self, dsn: str, min_size: int = 10, max_size: int = 30):
        """Initialize the PostgreSQL database connection pool.

        Args:
            dsn: Database connection string
            min_size: Minimum number of connections in the pool
            max_size: Maximum number of connections in the pool
        """
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.pool = None
        self.logger = logging.getLogger(__name__)

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
        )

    @asynccontextmanager
    async def _get_conn(self) -> AsyncGenerator[Any, asyncpg.Connection]:
        """Context manager to acquire a connection from the pool."""
        if not self.pool:
            raise RuntimeError("Database pool is not initialized")
        async with self.pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def _atomic(self) -> AsyncIterator[asyncpg.Connection]:
        """Context manager for atomic transactions."""
        async with self._get_conn() as conn:
            async with conn.transaction():
                yield conn

    async def close(self) -> None:
        """Close all database connections."""
        if self.pool:
            await self.pool.close()

    async def create_tables(self) -> None:
        schema_path = os.path.join(os.path.dirname(__file__), "psql.sql")

        try:
            with open(schema_path, "r") as f:
                schema_sql = f.read()

            async with self._get_conn() as conn:
                await conn.execute(schema_sql)
                self.logger.info("Database tables created successfully")
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            raise

    async def _fetchval(self, query: str, *args: Any) -> Any:
        async with self._get_conn() as conn:
            return await conn.fetchval(query, *args)

    async def get_product_barcodes(self) -> dict[str, int]:
        async with self._get_conn() as conn:
            rows = await conn.fetch("SELECT id, ean FROM products")
            return {row["ean"]: row["id"] for row in rows}

    async def get_chain_product_map(self, chain_id: int) -> dict[str, int]:
        async with self._get_conn() as conn:
            rows = await conn.fetch(
                """
                SELECT code, id FROM chain_products WHERE chain_id = $1
                """,
                chain_id,
            )
            return {row["code"]: row["id"] for row in rows}

    async def add_chain(self, chain: Chain) -> int:
        async with self._atomic() as conn:
            chain_id = await conn.fetchval(
                "SELECT id FROM chains WHERE code = $1",
                chain.code,
            )
            if chain_id is not None:
                return chain_id
            chain_id = await conn.fetchval(
                "INSERT INTO chains (code) VALUES ($1) RETURNING id",
                chain.code,
            )
            if chain_id is None:
                raise RuntimeError(f"Failed to insert chain {chain.code}")
            return chain_id

    async def add_store(self, store: Store) -> int:
        return await self._fetchval(
            """
            INSERT INTO stores (chain_id, code, type, address, city, zipcode)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (chain_id, code) DO UPDATE SET
                type = COALESCE($3, stores.type),
                address = COALESCE($4, stores.address),
                city = COALESCE($5, stores.city),
                zipcode = COALESCE($6, stores.zipcode)
            RETURNING id
            """,
            store.chain_id,
            store.code,
            store.type,
            store.address or None,
            store.city or None,
            store.zipcode or None,
        )

    async def add_ean(self, ean: str) -> int:
        """
        Add an empty product with only EAN barcode info.

        Args:
            ean: The EAN code to add.

        Returns:
            The database ID of the created product.
        """
        return await self._fetchval(
            "INSERT INTO products (ean) VALUES ($1) RETURNING id",
            ean,
        )

    async def add_many_prices(self, prices: list[Price]) -> int:
        async with self._atomic() as conn:
            await conn.execute(
                """
                CREATE TEMP TABLE temp_prices (
                    chain_product_id INTEGER,
                    store_id INTEGER,
                    price_date DATE,
                    regular_price DECIMAL(10, 2),
                    special_price DECIMAL(10, 2),
                    unit_price DECIMAL(10, 2),
                    best_price_30 DECIMAL(10, 2),
                    anchor_price DECIMAL(10, 2)
                )
                """
            )
            await conn.copy_records_to_table(
                "temp_prices",
                records=(
                    (
                        p.chain_product_id,
                        p.store_id,
                        p.price_date,
                        p.regular_price,
                        p.special_price,
                        p.unit_price,
                        p.best_price_30,
                        p.anchor_price,
                    )
                    for p in prices
                ),
            )
            result = await conn.execute(
                """
                INSERT INTO prices(
                    chain_product_id,
                    store_id,
                    price_date,
                    regular_price,
                    special_price,
                    unit_price,
                    best_price_30,
                    anchor_price
                )
                SELECT * from temp_prices
                ON CONFLICT DO NOTHING
                """
            )
            await conn.execute("DROP TABLE temp_prices")
            _, _, rowcount = result.split(" ")
            rowcount = int(rowcount)
            return rowcount

    async def add_many_chain_products(
        self,
        chain_products: List[ChainProduct],
    ) -> int:
        async with self._atomic() as conn:
            await conn.execute(
                """
                CREATE TEMP TABLE temp_chain_products (
                    chain_id INTEGER,
                    product_id INTEGER,
                    code VARCHAR(100),
                    name VARCHAR(255),
                    brand VARCHAR(255),
                    category VARCHAR(255),
                    unit VARCHAR(50),
                    quantity VARCHAR(50)
                )
                """
            )
            await conn.copy_records_to_table(
                "temp_chain_products",
                records=(
                    (
                        cp.chain_id,
                        cp.product_id,
                        cp.code,
                        cp.name,
                        cp.brand,
                        cp.category,
                        cp.unit,
                        cp.quantity,
                    )
                    for cp in chain_products
                ),
            )

            result = await conn.execute(
                """
                INSERT INTO chain_products(
                    chain_id,
                    product_id,
                    code,
                    name,
                    brand,
                    category,
                    unit,
                    quantity
                )
                SELECT * from temp_chain_products
                ON CONFLICT DO NOTHING
                """
            )
            await conn.execute("DROP TABLE temp_chain_products")

            _, _, rowcount = result.split(" ")
            rowcount = int(rowcount)
            return rowcount

    async def compute_chain_prices(self, date: date) -> None:
        async with self._get_conn() as conn:
            await conn.execute(
                """
                INSERT INTO chain_prices (
                    chain_product_id,
                    price_date,
                    min_price,
                    max_price,
                    avg_price
                )
                SELECT
                    chain_product_id,
                    price_date,
                    MIN(
                        LEAST(
                            COALESCE(regular_price, special_price),
                            COALESCE(special_price, regular_price)
                        )
                    ) AS min_price,
                    MAX(
                        LEAST(
                            COALESCE(regular_price, special_price),
                            COALESCE(special_price, regular_price)
                        )
                    ) AS max_price,
                    ROUND(
                        AVG(
                            LEAST(
                                COALESCE(regular_price, special_price),
                                COALESCE(special_price, regular_price)
                            )
                        ),
                        2
                    ) AS avg_price
                FROM prices
                WHERE price_date = $1
                GROUP BY chain_product_id, price_date
                ON CONFLICT (chain_product_id, price_date)
                DO UPDATE SET
                    min_price = EXCLUDED.min_price,
                    max_price = EXCLUDED.max_price,
                    avg_price = EXCLUDED.avg_price;

                """,
                date,
            )

    async def get_user_by_api_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        async with self._get_conn() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, name, api_key, is_active, created_at
                FROM users
                WHERE
                    api_key = $1 AND
                    is_active = TRUE
                """,
                api_key,
            )

            if row:
                return dict(row)
            return None
