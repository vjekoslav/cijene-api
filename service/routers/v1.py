from decimal import Decimal
from typing import cast
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from datetime import date

from service.config import settings

router = APIRouter()
db = settings.get_db()


class ListChainsResponse(BaseModel):
    """List chains response schema."""

    chains: list[str] = Field(..., description="List of retail chain codes.")


@router.get("/chains/")
async def list_chains() -> ListChainsResponse:
    """List all available chains."""
    chains = await db.list_chains()
    return ListChainsResponse(chains=[chain.code for chain in chains])


class StoreResponse(BaseModel):
    """Store response schema."""

    chain_code: str = Field(..., description="Code of the retail chain.")
    code: str = Field(..., description="Unique code of the store.")
    type: str | None = Field(
        ...,
        description="Type of the store (e.g., supermarket, hypermarket).",
    )
    address: str | None = Field(..., description="Physical address of the store.")
    city: str | None = Field(..., description="City where the store is located.")
    zipcode: str | None = Field(..., description="Postal code of the store location.")


class ListStoresResponse(BaseModel):
    """List stores response schema."""

    stores: list[StoreResponse] = Field(
        ..., description="List stores for the specified chain."
    )


@router.get("/{chain_code}/stores/")
async def list_stores(chain_code: str) -> ListStoresResponse:
    """
    List all stores for a particular chain.

    TODO: Allow filtering by store type and location.
    """
    stores = await db.list_stores(chain_code)

    if not stores:
        raise HTTPException(status_code=404, detail=f"No chain {chain_code}")

    return ListStoresResponse(
        stores=[
            StoreResponse(
                chain_code=chain_code,
                code=store.code,
                type=store.type,
                address=store.address,
                city=store.city,
                zipcode=store.zipcode,
            )
            for store in stores
        ]
    )


class ProductInfoResponse(BaseModel):
    """Basic product information response schema."""

    ean: str = Field(..., description="EAN barcode of the product.")
    brand: str | None = Field(..., description="Brand of the product.")
    name: str | None = Field(..., description="Name of the product.")
    quantity: str | None = Field(..., description="Quantity of the product.")
    unit: str | None = Field(..., description="Unit of the product.")


class ChainProductPriceResponse(BaseModel):
    """Chain product with price information response schema."""

    chain: str = Field(..., description="Chain code.")
    code: str = Field(..., description="Product code within the chain.")
    name: str = Field(..., description="Product name within the chain.")
    brand: str | None = Field(..., description="Product brand within the chain.")
    category: str | None = Field(..., description="Product category within the chain.")
    unit: str | None = Field(..., description="Product unit within the chain.")
    quantity: str | None = Field(..., description="Product quantity within the chain.")
    min_price: Decimal = Field(..., description="Minimum price across chain stores.")
    max_price: Decimal = Field(..., description="Maximum price across chain stores.")
    avg_price: Decimal = Field(..., description="Average price across chain stores.")


class ProductResponse(BaseModel):
    """Product with chains and prices response schema."""

    product: ProductInfoResponse = Field(..., description="Basic product information.")
    chains: list[ChainProductPriceResponse] = Field(
        ..., description="Chain-specific product and price information."
    )


@router.get("/products/{ean}/")
async def get_product(ean: str) -> ProductResponse:
    """
    Get product information including chain products and prices by EAN.

    Args:
        ean: The EAN barcode of the product.

    Returns:
        Product information with chain products and price data.
    """
    product = await db.get_product_by_ean(ean)
    if not product:
        raise HTTPException(status_code=404, detail=f"Product with EAN {ean} not found")

    product_id = cast(int, product.id)

    chain_id_to_code = {
        cast(int, chain.id): chain.code for chain in await db.list_chains()
    }

    chain_products = await db.get_chain_products_for_product(product_id)
    chain_responses = {}

    for cp in chain_products:
        cpr = cp.to_dict()
        cpr["chain"] = chain_id_to_code[cpr.pop("chain_id")]
        chain_responses[cp.chain_id] = cpr

    today = date.today()

    prices = await db.get_product_prices(product_id, today)

    for p in prices:
        cpr = chain_responses.get(p["chain_id"])
        if not cpr:
            continue
        cpr["min_price"] = p["min_price"]
        cpr["max_price"] = p["max_price"]
        cpr["avg_price"] = p["avg_price"]

    # Remove chain products that do not have corresponding chain prices
    filtered_chain_responses = [
        cpr
        for cpr in chain_responses.values()
        if ("min_price" in cpr and "max_price" in cpr and "avg_price" in cpr)
    ]

    # Fixup global product brand and name using chain data
    # Logic here is that the longest string is the most likely to be most useful

    product_brand = product.brand
    if not product_brand:
        chain_brands = [cpr.get("brand") for cpr in filtered_chain_responses]
        chain_brands = [b for b in chain_brands if b]
        chain_brands.sort(key=lambda x: len(x))
        if chain_brands:
            product_brand = chain_brands[0].capitalize()

    product_name = product.name
    if not product_name:
        chain_names = [cpr.get("name") for cpr in filtered_chain_responses]
        chain_names = [n for n in chain_names if n]
        chain_names.sort(key=lambda x: len(x), reverse=True)
        if chain_names:
            product_name = chain_names[0].capitalize()

    return ProductResponse(
        product=ProductInfoResponse(
            ean=product.ean,
            brand=product_brand,
            name=product_name,
            quantity=str(product.quantity) if product.quantity else None,
            unit=product.unit,
        ),
        chains=[ChainProductPriceResponse(**cpr) for cpr in filtered_chain_responses],
    )
