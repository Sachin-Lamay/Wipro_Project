"""
E-Commerce Customer Behavior & Recommendation System
ETL Pipeline - Data Generation & Extraction
Generates realistic synthetic CSV data for all entities
"""

import csv
import json
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

# ─── Config ───────────────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)

N_CUSTOMERS   = 5_000
N_PRODUCTS    = 500
N_CATEGORIES  = 20
N_ORDERS      = 30_000
N_REVIEWS     = 15_000
N_CLICKSTREAM = 100_000

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─── Helpers ──────────────────────────────────────────────────────────────────
def rand_id(prefix: str, n: int = 8) -> str:
    return f"{prefix}-{''.join(random.choices(string.hexdigits[:16], k=n)).upper()}"

def rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

# ─── Reference Data ───────────────────────────────────────────────────────────
COUNTRIES = [
    ("United States", ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
    ("United Kingdom", ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"]),
    ("Germany",        ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"]),
    ("France",         ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]),
    ("Canada",         ["Toronto", "Montreal", "Vancouver", "Calgary", "Ottawa"]),
    ("Australia",      ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]),
    ("Japan",          ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Sapporo"]),
    ("India",          ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad"]),
]

CATEGORIES = [
    ("CAT-001", "Electronics",    "Smartphones"),
    ("CAT-002", "Electronics",    "Laptops"),
    ("CAT-003", "Electronics",    "Tablets"),
    ("CAT-004", "Electronics",    "Accessories"),
    ("CAT-005", "Clothing",       "Men's Wear"),
    ("CAT-006", "Clothing",       "Women's Wear"),
    ("CAT-007", "Clothing",       "Kids"),
    ("CAT-008", "Home & Garden",  "Furniture"),
    ("CAT-009", "Home & Garden",  "Kitchen"),
    ("CAT-010", "Home & Garden",  "Decor"),
    ("CAT-011", "Sports",         "Outdoor"),
    ("CAT-012", "Sports",         "Fitness"),
    ("CAT-013", "Books",          "Fiction"),
    ("CAT-014", "Books",          "Non-Fiction"),
    ("CAT-015", "Beauty",         "Skincare"),
    ("CAT-016", "Beauty",         "Makeup"),
    ("CAT-017", "Toys",           "Educational"),
    ("CAT-018", "Toys",           "Action Figures"),
    ("CAT-019", "Food",           "Organic"),
    ("CAT-020", "Automotive",     "Parts"),
]

BRANDS = ["TechPro", "StyleCo", "HomePlus", "SportZen", "ReadMore",
          "GlowUp", "KidsWorld", "AutoParts", "EcoLife", "LuxBrand",
          "ValuePick", "TrendSet", "NatureCo", "QuickFit", "GadgetHub"]

SEGMENTS   = ["High Value", "Mid Value", "Low Value", "New Customer", "Churned"]
CHANNELS   = ["web", "mobile_app", "marketplace", "social", "email_campaign"]
STATUSES   = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "SHIPPED",
              "PROCESSING", "CANCELLED", "REFUNDED"]
SHIP_METHODS = ["Standard", "Express", "Overnight", "Free Shipping", "Click & Collect"]
PAY_METHODS  = ["credit_card", "debit_card", "paypal", "apple_pay",
                "google_pay", "bank_transfer", "buy_now_pay_later"]
GATEWAYS     = ["Stripe", "PayPal", "Adyen", "Square", "Braintree"]
DEVICES      = ["desktop", "mobile", "tablet"]
BROWSERS     = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
EVENT_TYPES  = ["page_view", "product_view", "add_to_cart", "remove_from_cart",
                "checkout", "purchase", "search", "wishlist_add", "review_submit"]

# ─── Generators ───────────────────────────────────────────────────────────────
def generate_customers(n: int) -> list[dict]:
    first_names = ["James","Mary","Robert","Patricia","John","Jennifer","Michael",
                   "Linda","David","Barbara","Liam","Emma","Noah","Olivia","William",
                   "Ava","Oliver","Sophia","Elijah","Isabella","Charlotte","Amelia"]
    last_names  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
                   "Davis","Wilson","Taylor","Anderson","Thomas","Jackson","White",
                   "Harris","Martin","Thompson","Young","Walker","Allen"]
    customers = []
    start = datetime(2019, 1, 1)
    end   = datetime(2024, 12, 31)
    for i in range(n):
        country, cities = random.choice(COUNTRIES)
        dob = rand_date(datetime(1960,1,1), datetime(2003,12,31))
        customers.append({
            "customer_id":       rand_id("CUST"),
            "first_name":        random.choice(first_names),
            "last_name":         random.choice(last_names),
            "email":             f"user{i+1}@{'gmail' if i%3==0 else 'yahoo' if i%3==1 else 'email'}.com",
            "phone":             f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            "date_of_birth":     fmt_date(dob),
            "gender":            random.choice(["M","F","F","M","Other"]),
            "country":           country,
            "city":              random.choice(cities),
            "zip_code":          f"{random.randint(10000,99999)}",
            "registration_date": fmt(rand_date(start, end)),
            "segment":           random.choice(SEGMENTS),
        })
    return customers

def generate_products(n: int) -> list[dict]:
    products = []
    for i in range(n):
        cat_id, cat_name, subcat = random.choice(CATEGORIES)
        base_price  = round(random.uniform(5.0, 2000.0), 2)
        cost_price  = round(base_price * random.uniform(0.35, 0.70), 2)
        products.append({
            "product_id":     rand_id("PROD"),
            "product_name":   f"{random.choice(BRANDS)} {subcat} Model-{random.randint(100,999)}",
            "category_id":    cat_id,
            "category_name":  cat_name,
            "subcategory_name": subcat,
            "brand":          random.choice(BRANDS),
            "sku":            f"SKU-{random.randint(100000,999999)}",
            "unit_price":     base_price,
            "cost_price":     cost_price,
            "weight_kg":      round(random.uniform(0.1, 25.0), 2),
            "is_active":      random.choice([True, True, True, False]),
            "launch_date":    fmt_date(rand_date(datetime(2018,1,1), datetime(2024,6,1))),
        })
    return products

def generate_orders(n: int, customers: list[dict], products: list[dict]) -> tuple[list,list]:
    orders = []
    items  = []
    cust_ids = [c["customer_id"] for c in customers]
    prod_map = {p["product_id"]: p for p in products}
    prod_ids = list(prod_map.keys())

    for i in range(n):
        order_date  = rand_date(datetime(2021,1,1), datetime(2024,12,31))
        cust_id     = random.choice(cust_ids)
        channel     = random.choice(CHANNELS)
        ship_cost   = round(random.choice([0, 0, 4.99, 9.99, 14.99]), 2)
        n_items     = random.randint(1, 6)

        order_items = []
        subtotal    = 0.0
        for _ in range(n_items):
            pid         = random.choice(prod_ids)
            prod        = prod_map[pid]
            qty         = random.randint(1, 4)
            unit_price  = prod["unit_price"]
            disc_pct    = random.choice([0, 0, 5, 10, 15, 20]) / 100
            line_total  = round(qty * unit_price * (1 - disc_pct), 2)
            subtotal   += line_total
            order_items.append({
                "order_item_id": rand_id("ITEM"),
                "order_id":      None,  # set after order_id created
                "product_id":    pid,
                "quantity":      qty,
                "unit_price":    unit_price,
                "discount_pct":  disc_pct * 100,
                "line_total":    line_total,
            })

        disc_amount  = round(subtotal * random.choice([0, 0, 0.05, 0.10]), 2)
        tax_amount   = round((subtotal - disc_amount) * 0.08, 2)
        total_amount = round(subtotal - disc_amount + ship_cost + tax_amount, 2)
        order_id     = rand_id("ORD")

        for oi in order_items:
            oi["order_id"] = order_id
            items.append(oi)

        orders.append({
            "order_id":       order_id,
            "customer_id":    cust_id,
            "order_date":     fmt(order_date),
            "status":         random.choice(STATUSES),
            "shipping_method": random.choice(SHIP_METHODS),
            "shipping_cost":  ship_cost,
            "discount_amount": disc_amount,
            "tax_amount":     tax_amount,
            "total_amount":   total_amount,
            "currency":       "USD",
            "channel":        channel,
        })
    return orders, items

def generate_payments(orders: list[dict]) -> list[dict]:
    payments = []
    for o in orders:
        if o["status"] in ("PROCESSING", "CANCELLED"):
            continue
        pay_date = datetime.strptime(o["order_date"], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(1,30))
        payments.append({
            "payment_id":     rand_id("PAY"),
            "order_id":       o["order_id"],
            "payment_date":   fmt(pay_date),
            "payment_method": random.choice(PAY_METHODS),
            "payment_status": "SUCCESS" if o["status"] != "REFUNDED" else "REFUNDED",
            "amount":         o["total_amount"],
            "gateway":        random.choice(GATEWAYS),
            "transaction_ref": rand_id("TXN", 16),
        })
    return payments

def generate_reviews(n: int, orders: list[dict], products: list[dict]) -> list[dict]:
    prod_ids  = [p["product_id"] for p in products]
    cust_ids  = [o["customer_id"] for o in orders if o["status"] == "COMPLETED"]
    titles    = ["Great product!", "Highly recommend", "Good value", "Not as expected",
                 "Excellent quality", "Decent", "Amazing!", "Could be better", "Perfect", "Love it"]
    reviews   = []
    for _ in range(n):
        rating = random.choices([1,2,3,4,5], weights=[5,5,15,30,45])[0]
        reviews.append({
            "review_id":         rand_id("REV"),
            "product_id":        random.choice(prod_ids),
            "customer_id":       random.choice(cust_ids),
            "rating":            rating,
            "review_title":      random.choice(titles),
            "review_body":       f"Rating {rating}/5 - {'Excellent experience' if rating >= 4 else 'Average experience' if rating == 3 else 'Disappointing'}.",
            "review_date":       fmt_date(rand_date(datetime(2021,1,1), datetime(2024,12,31))),
            "helpful_votes":     random.randint(0, 150),
            "verified_purchase": random.choice([True, True, False]),
        })
    return reviews

def generate_clickstream(n: int, customers: list[dict], products: list[dict]) -> list[dict]:
    cust_ids = [c["customer_id"] for c in customers]
    prod_ids = [p["product_id"] for p in products]
    pages    = ["/", "/products", "/cart", "/checkout", "/account", "/search", "/sale"]
    events   = []
    for i in range(n):
        ts       = rand_date(datetime(2024,1,1), datetime(2024,12,31))
        cust_id  = random.choice(cust_ids) if random.random() > 0.15 else None
        prod_id  = random.choice(prod_ids) if random.random() > 0.5 else None
        etype    = random.choices(EVENT_TYPES, weights=[30,20,15,5,8,7,10,3,2])[0]
        payload  = {
            "event_type": etype,
            "session_id": rand_id("SES"),
            "user_agent": {
                "device":  random.choice(DEVICES),
                "browser": random.choice(BROWSERS),
            },
            "geo": {"country": random.choice(COUNTRIES)[0]},
            "product": {"id": prod_id, "price": round(random.uniform(5,2000),2)} if prod_id else None,
            "cart":    {"items": [prod_id] if prod_id else [], "total": round(random.uniform(10,500),2)},
            "session": {"duration_seconds": random.randint(5,3600)},
        }
        events.append({
            "event_id":        rand_id("EVT"),
            "session_id":      payload["session_id"],
            "customer_id":     cust_id or "",
            "event_type":      etype,
            "page_url":        random.choice(pages),
            "product_id":      prod_id or "",
            "event_timestamp": fmt(ts),
            "device_type":     payload["user_agent"]["device"],
            "browser":         payload["user_agent"]["browser"],
            "referrer":        random.choice(["google.com","direct","facebook.com","email","instagram.com",""]),
            "raw_json":        json.dumps(payload),
        })
    return events

# ─── Write CSV ─────────────────────────────────────────────────────────────────
def write_csv(records: list[dict], filename: str):
    if not records:
        return
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"  ✓  {filename:40s} {len(records):>8,} rows  →  {path}")

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  E-Commerce Data Generator")
    print("=" * 65)

    print("\n[1/6] Generating customers …")
    customers = generate_customers(N_CUSTOMERS)
    write_csv(customers, "customers.csv")

    print("\n[2/6] Generating products …")
    products = generate_products(N_PRODUCTS)
    write_csv(products, "products.csv")

    print("\n[3/6] Generating orders & order_items …")
    orders, items = generate_orders(N_ORDERS, customers, products)
    write_csv(orders, "orders.csv")
    write_csv(items,  "order_items.csv")

    print("\n[4/6] Generating payments …")
    payments = generate_payments(orders)
    write_csv(payments, "payments.csv")

    print("\n[5/6] Generating reviews …")
    reviews = generate_reviews(N_REVIEWS, orders, products)
    write_csv(reviews, "reviews.csv")

    print("\n[6/6] Generating clickstream events …")
    events = generate_clickstream(N_CLICKSTREAM, customers, products)
    write_csv(events, "clickstream.csv")

    print("\n" + "=" * 65)
    print("  Data generation complete!")
    print("=" * 65)

if __name__ == "__main__":
    main()
