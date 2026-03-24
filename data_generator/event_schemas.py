"""
event_schemas.py
----------------
Pydantic data models for all e-commerce events.
Two core event types:
  1. UserActivityEvent  - page views, searches, cart actions, etc.
  2. PurchaseEvent      - completed orders with line items
"""

from __future__ import annotations
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import uuid


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------

class DeviceInfo(BaseModel):
    device_type: str          # mobile | desktop | tablet
    os: str                   # iOS | Android | Windows | macOS | Linux
    browser: str              # Chrome | Safari | Firefox | Edge
    app_version: str          # e.g. "2.4.1"
    screen_resolution: str    # e.g. "1920x1080"


class GeoLocation(BaseModel):
    country: str
    country_code: str
    city: str
    state: str
    latitude: float
    longitude: float
    timezone: str


class ProductItem(BaseModel):
    product_id: str
    product_name: str
    category: str
    subcategory: str
    brand: str
    sku: str
    price: float
    quantity: int
    discount_pct: float       # 0.0 – 0.5
    final_price: float        # price * quantity * (1 - discount_pct)


class PaymentInfo(BaseModel):
    method: str               # credit_card | debit_card | paypal | upi | wallet
    provider: str             # Visa | Mastercard | PayPal | Stripe | etc.
    last_four: Optional[str]  # last 4 digits for cards
    transaction_id: str
    currency: str             # USD | EUR | GBP | INR | etc.
    status: str               # success | pending | failed


# ---------------------------------------------------------------------------
# Core event models
# ---------------------------------------------------------------------------

class UserActivityEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str           # page_view | search | add_to_cart | remove_from_cart
                              # wishlist_add | product_view | checkout_start
    timestamp: str
    user_id: str
    session_id: str
    anonymous: bool

    # User profile (populated for logged-in users)
    user_segment: Optional[str]   # new | returning | vip | at_risk
    account_age_days: Optional[int]

    # Page / interaction context
    page_url: str
    referrer: Optional[str]
    search_query: Optional[str]   # only for search events
    product_id: Optional[str]     # only for product_view / cart events
    category: Optional[str]

    # Session metadata
    session_duration_sec: int
    page_views_in_session: int
    is_bounce: bool

    device: DeviceInfo
    geo: GeoLocation

    # A/B test assignments
    ab_test_variant: Optional[str]  # control | variant_a | variant_b

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class PurchaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "purchase"
    timestamp: str
    user_id: str
    session_id: str
    order_id: str

    # Order details
    items: List[ProductItem]
    item_count: int
    subtotal: float
    tax: float
    shipping_cost: float
    discount_amount: float
    order_total: float
    coupon_code: Optional[str]

    # Fulfilment
    shipping_method: str      # standard | express | overnight | pickup
    estimated_delivery_days: int
    warehouse_id: str
    is_gift: bool

    payment: PaymentInfo
    device: DeviceInfo
    geo: GeoLocation

    # Customer lifetime value context
    customer_order_count: int       # how many orders this customer has made total
    customer_total_spend: float

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}