"""
faker_generator.py
------------------
Generates realistic fake e-commerce events using the Faker library.

Usage (standalone – dumps JSON to stdout):
    python faker_generator.py --count 50 --type all
    python faker_generator.py --count 10 --type purchase
    python faker_generator.py --count 10 --type activity

Usage (imported by producer):
    from data_generator.faker_generator import EcommerceEventGenerator
    gen = EcommerceEventGenerator(seed=42)
    event = gen.generate_user_activity()
    event = gen.generate_purchase()
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

from decimal import Decimal

from faker import Faker

fake = Faker()
Faker.seed(0)


class DecimalEncoder(json.JSONEncoder):
    """Serialize Decimal → float so json.dumps never raises TypeError."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


# ---------------------------------------------------------------------------
# Static lookup tables
# ---------------------------------------------------------------------------

PRODUCT_CATALOG = [
    {"product_id": "P001", "name": "Wireless Noise-Cancelling Headphones", "category": "Electronics",    "subcategory": "Audio",        "brand": "SoundMax",   "sku": "SM-WNC-001", "base_price": 149.99},
    {"product_id": "P002", "name": "Ultra-Slim Laptop 15\"",               "category": "Electronics",    "subcategory": "Computers",    "brand": "TechPro",    "sku": "TP-ULT-015", "base_price": 999.99},
    {"product_id": "P003", "name": "Ergonomic Office Chair",               "category": "Furniture",      "subcategory": "Seating",      "brand": "ComfortPlus","sku": "CP-EOC-003", "base_price": 349.99},
    {"product_id": "P004", "name": "Organic Cotton T-Shirt",               "category": "Clothing",       "subcategory": "Tops",         "brand": "EcoWear",    "sku": "EW-OCT-004", "base_price":  29.99},
    {"product_id": "P005", "name": "Stainless Steel Water Bottle 32oz",    "category": "Sports",         "subcategory": "Hydration",    "brand": "HydroLife", "sku": "HL-SSW-005", "base_price":  34.99},
    {"product_id": "P006", "name": "4K Smart TV 55\"",                     "category": "Electronics",    "subcategory": "Televisions",  "brand": "VisionTech", "sku": "VT-4KT-055", "base_price": 699.99},
    {"product_id": "P007", "name": "Running Shoes Pro",                    "category": "Sports",         "subcategory": "Footwear",     "brand": "SpeedStep",  "sku": "SS-RSP-007", "base_price":  89.99},
    {"product_id": "P008", "name": "Instant Pot Pressure Cooker 6Qt",      "category": "Kitchen",        "subcategory": "Appliances",   "brand": "QuickCook",  "sku": "QC-IPC-006", "base_price":  79.99},
    {"product_id": "P009", "name": "Yoga Mat Premium",                     "category": "Sports",         "subcategory": "Yoga",         "brand": "ZenFit",     "sku": "ZF-YMP-009", "base_price":  45.99},
    {"product_id": "P010", "name": "Vitamin D3 + K2 Supplement",           "category": "Health",         "subcategory": "Vitamins",     "brand": "VitaLife",   "sku": "VL-VDK-010", "base_price":  19.99},
    {"product_id": "P011", "name": "Mechanical Keyboard RGB",              "category": "Electronics",    "subcategory": "Peripherals",  "brand": "TypeMaster", "sku": "TM-MKR-011", "base_price": 129.99},
    {"product_id": "P012", "name": "Skincare Serum Vitamin C",             "category": "Beauty",         "subcategory": "Skincare",     "brand": "GlowUp",     "sku": "GU-SVC-012", "base_price":  49.99},
    {"product_id": "P013", "name": "Pet Food Premium Dry 15lb",            "category": "Pet Supplies",   "subcategory": "Dog Food",     "brand": "PawPerfect","sku": "PP-PFD-013", "base_price":  54.99},
    {"product_id": "P014", "name": "Standing Desk Converter",              "category": "Furniture",      "subcategory": "Desks",        "brand": "StandRight", "sku": "SR-SDC-014", "base_price": 189.99},
    {"product_id": "P015", "name": "Bluetooth Speaker Waterproof",         "category": "Electronics",    "subcategory": "Audio",        "brand": "BoomBox",    "sku": "BB-BSW-015", "base_price":  59.99},
]

ACTIVITY_EVENTS = [
    "page_view", "page_view", "page_view",   # higher weight
    "product_view", "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "wishlist_add",
    "checkout_start",
]

DEVICE_TYPES   = ["mobile", "mobile", "desktop", "desktop", "tablet"]
OS_MAP         = {"mobile": ["iOS", "Android"], "desktop": ["Windows", "macOS", "Linux"], "tablet": ["iOS", "Android"]}
BROWSERS       = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "upi", "wallet"]
PAYMENT_PROVIDERS = {
    "credit_card":  ["Visa", "Mastercard", "Amex"],
    "debit_card":   ["Visa", "Mastercard"],
    "paypal":       ["PayPal"],
    "upi":          ["PhonePe", "GooglePay", "Paytm"],
    "wallet":       ["Apple Pay", "Google Pay", "Samsung Pay"],
}
CURRENCIES     = ["USD", "USD", "USD", "EUR", "GBP", "INR", "CAD", "AUD"]
SHIPPING_METHODS = ["standard", "standard", "express", "overnight", "pickup"]
USER_SEGMENTS  = ["new", "returning", "returning", "vip", "at_risk"]
WAREHOUSES     = ["WH-US-EAST", "WH-US-WEST", "WH-EU-CENTRAL", "WH-APAC-SG"]

GEO_DATA = [
    {"country": "United States",  "cc": "US", "cities": [("New York","NY"),("Los Angeles","CA"),("Chicago","IL"),("Houston","TX")]},
    {"country": "United Kingdom", "cc": "GB", "cities": [("London","ENG"),("Manchester","ENG"),("Birmingham","ENG")]},
    {"country": "Germany",        "cc": "DE", "cities": [("Berlin","BE"),("Munich","BY"),("Hamburg","HH")]},
    {"country": "India",          "cc": "IN", "cities": [("Bangalore","KA"),("Mumbai","MH"),("Delhi","DL"),("Hyderabad","TS")]},
    {"country": "Canada",         "cc": "CA", "cities": [("Toronto","ON"),("Vancouver","BC"),("Montreal","QC")]},
]

REFERRERS = [
    "https://www.google.com", "https://www.google.com",
    "https://www.facebook.com", "https://www.instagram.com",
    "https://www.youtube.com", None, None,
    "https://www.bing.com", "email_campaign",
]


# ---------------------------------------------------------------------------
# Generator class
# ---------------------------------------------------------------------------

class EcommerceEventGenerator:
    """
    Stateful generator that maintains a pool of users and sessions to
    produce realistic, correlated fake e-commerce events.
    """

    def __init__(self, seed: int = 42, num_users: int = 5_000):
        random.seed(seed)
        fake.seed_instance(seed)

        # Pre-generate user pool (stable across calls for realistic sessions)
        self._users = self._build_user_pool(num_users)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_user_activity(self) -> Dict[str, Any]:
        user     = self._pick_user()
        device   = self._make_device()
        geo      = self._make_geo()
        event_type = random.choice(ACTIVITY_EVENTS)

        product = random.choice(PRODUCT_CATALOG) if event_type in (
            "product_view", "add_to_cart", "remove_from_cart", "wishlist_add"
        ) else None

        event = {
            "event_id":              str(uuid.uuid4()),
            "event_type":            event_type,
            "timestamp":             self._now_iso(),
            "user_id":               user["user_id"],
            "session_id":            str(uuid.uuid4()),
            "anonymous":             user["anonymous"],
            "user_segment":          user.get("segment"),
            "account_age_days":      user.get("account_age_days"),
            "page_url":              self._make_url(event_type, product),
            "referrer":              random.choice(REFERRERS),
            "search_query":          fake.bs().lower() if event_type == "search" else None,
            "product_id":            product["product_id"] if product else None,
            "category":              product["category"] if product else None,
            "session_duration_sec":  random.randint(5, 1800),
            "page_views_in_session": random.randint(1, 25),
            "is_bounce":             random.random() < 0.35,
            "device":                device,
            "geo":                   geo,
            "ab_test_variant":       random.choice(["control", "variant_a", "variant_b", None]),
        }
        return event

    def generate_purchase(self) -> Dict[str, Any]:
        user        = self._pick_user(logged_in=True)
        device      = self._make_device()
        geo         = self._make_geo()
        items       = self._make_order_items()
        subtotal    = round(sum(i["final_price"] for i in items), 2)
        tax         = round(subtotal * random.uniform(0.05, 0.12), 2)
        shipping    = round(random.choice([0, 4.99, 7.99, 12.99, 19.99]), 2)
        discount    = round(subtotal * random.uniform(0, 0.15), 2) if random.random() < 0.3 else 0.0
        total       = round(subtotal + tax + shipping - discount, 2)
        pay_method  = random.choice(PAYMENT_METHODS)
        currency    = random.choice(CURRENCIES)

        event = {
            "event_id":                str(uuid.uuid4()),
            "event_type":              "purchase",
            "timestamp":               self._now_iso(),
            "user_id":                 user["user_id"],
            "session_id":              str(uuid.uuid4()),
            "order_id":                f"ORD-{fake.numerify('########')}",
            "items":                   items,
            "item_count":              sum(i["quantity"] for i in items),
            "subtotal":                subtotal,
            "tax":                     tax,
            "shipping_cost":           shipping,
            "discount_amount":         discount,
            "order_total":             total,
            "coupon_code":             f"SAVE{random.randint(10,30)}" if discount > 0 else None,
            "shipping_method":         random.choice(SHIPPING_METHODS),
            "estimated_delivery_days": random.randint(1, 10),
            "warehouse_id":            random.choice(WAREHOUSES),
            "is_gift":                 random.random() < 0.12,
            "payment": {
                "method":          pay_method,
                "provider":        random.choice(PAYMENT_PROVIDERS[pay_method]),
                "last_four":       fake.numerify("####") if pay_method in ("credit_card","debit_card") else None,
                "transaction_id":  str(uuid.uuid4()),
                "currency":        currency,
                "status":          random.choices(["success","pending","failed"], weights=[90,7,3])[0],
            },
            "device":                  device,
            "geo":                     geo,
            "customer_order_count":    user.get("order_count", 1),
            "customer_total_spend":    round(user.get("total_spend", 0) + total, 2),
        }
        return event

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_user_pool(self, n: int):
        users = []
        for _ in range(n):
            anon = random.random() < 0.25
            users.append({
                "user_id":         str(uuid.uuid4()) if not anon else f"anon_{fake.md5()[:12]}",
                "anonymous":       anon,
                "segment":         random.choice(USER_SEGMENTS) if not anon else None,
                "account_age_days":random.randint(1, 2000) if not anon else None,
                "order_count":     random.randint(1, 50) if not anon else 0,
                "total_spend":     round(random.uniform(0, 5000), 2) if not anon else 0.0,
            })
        return users

    def _pick_user(self, logged_in: bool = False):
        pool = [u for u in self._users if not u["anonymous"]] if logged_in else self._users
        return random.choice(pool)

    def _make_device(self) -> Dict[str, Any]:
        dtype = random.choice(DEVICE_TYPES)
        return {
            "device_type":       dtype,
            "os":                random.choice(OS_MAP[dtype]),
            "browser":           random.choice(BROWSERS),
            "app_version":       f"{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}",
            "screen_resolution": random.choice(["1920x1080","1366x768","390x844","375x667","2560x1440","1280x800"]),
        }

    def _make_geo(self) -> Dict[str, Any]:
        region  = random.choice(GEO_DATA)
        city, state = random.choice(region["cities"])
        return {
            "country":      region["country"],
            "country_code": region["cc"],
            "city":         city,
            "state":        state,
            "latitude":     round(fake.latitude(), 6),
            "longitude":    round(fake.longitude(), 6),
            "timezone":     fake.timezone(),
        }

    def _make_order_items(self):
        n_items = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 12, 8, 5])[0]
        chosen  = random.sample(PRODUCT_CATALOG, min(n_items, len(PRODUCT_CATALOG)))
        items   = []
        for p in chosen:
            qty      = random.choices([1, 2, 3], weights=[75, 18, 7])[0]
            disc     = round(random.choice([0.0, 0.0, 0.05, 0.10, 0.15, 0.20, 0.30]), 2)
            final    = round(p["base_price"] * qty * (1 - disc), 2)
            items.append({
                "product_id":   p["product_id"],
                "product_name": p["name"],
                "category":     p["category"],
                "subcategory":  p["subcategory"],
                "brand":        p["brand"],
                "sku":          p["sku"],
                "price":        p["base_price"],
                "quantity":     qty,
                "discount_pct": disc,
                "final_price":  final,
            })
        return items

    def _make_url(self, event_type: str, product) -> str:
        base = "https://shop.example.com"
        if event_type == "page_view":
            return random.choice([base, f"{base}/deals", f"{base}/new-arrivals", f"{base}/categories"])
        if event_type == "search":
            return f"{base}/search?q={fake.word()}"
        if product:
            return f"{base}/products/{product['product_id'].lower()}"
        return base

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate fake e-commerce events")
    parser.add_argument("--count", type=int, default=10, help="Number of events to generate")
    parser.add_argument("--type",  choices=["activity", "purchase", "all"], default="all")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = parser.parse_args()

    gen    = EcommerceEventGenerator(seed=42)
    indent = 2 if args.pretty else None

    for i in range(args.count):
        if args.type == "activity":
            event = gen.generate_user_activity()
        elif args.type == "purchase":
            event = gen.generate_purchase()
        else:
            event = gen.generate_purchase() if random.random() < 0.15 else gen.generate_user_activity()
        print(json.dumps(event, indent=indent, cls=DecimalEncoder))
