from typing import Optional
from datetime import date
from decimal import Decimal

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class User:
    name: str
    api_key: str
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class Chain:
    code: str
    id: int | None = None


@dataclass(frozen=True, slots=True)
class Store:
    chain_id: int
    code: str
    type: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    zipcode: Optional[str] = None


@dataclass(frozen=True, slots=True)
class Product:
    ean: str
    brand: Optional[str] = None
    name: Optional[str] = None
    quantity: Optional[Decimal] = None
    unit: Optional[str] = None
    id: int | None = None


@dataclass(frozen=True, slots=True)
class ChainProduct:
    chain_id: int
    product_id: int
    code: str
    name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    unit: Optional[str] = None
    quantity: Optional[str] = None

    def to_dict(self):
        return {k: getattr(self, k) for k in self.__slots__}


@dataclass(frozen=True, slots=True)
class Price:
    chain_product_id: int
    store_id: int
    price_date: date
    regular_price: Optional[Decimal] = None
    special_price: Optional[Decimal] = None
    unit_price: Optional[Decimal] = None
    best_price_30: Optional[Decimal] = None
    anchor_price: Optional[Decimal] = None
