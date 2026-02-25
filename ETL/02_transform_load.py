"""
E-Commerce ETL Pipeline - Transform & Load
Cleans CSV data and prepares for Snowflake COPY INTO
Uses Pandas for data validation, deduplication, type coercion
"""

import csv
import json
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd
import numpy as np

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl")

RAW_DIR  = Path(__file__).parent.parent / "data" / "raw"
PROC_DIR = Path(__file__).parent.parent / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

# ─── Data Quality Report ──────────────────────────────────────────────────────
class DQReport:
    def __init__(self, table: str):
        self.table        = table
        self.total_rows   = 0
        self.valid_rows   = 0
        self.issues: list[dict] = []

    def add_issue(self, rule: str, count: int, details: str = ""):
        self.issues.append({"rule": rule, "count": count, "details": details})
        log.warning("DQ [%s] %-30s  bad_rows=%d  %s", self.table, rule, count, details)

    def summary(self) -> dict:
        return {
            "table":      self.table,
            "total_rows": self.total_rows,
            "valid_rows": self.valid_rows,
            "pass_rate":  round(self.valid_rows / max(self.total_rows,1) * 100, 2),
            "issues":     self.issues,
        }

# ─── Transform: Customers ─────────────────────────────────────────────────────
def transform_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, DQReport]:
    rpt = DQReport("customers")
    rpt.total_rows = len(df)

    # Deduplicate
    dupes = df.duplicated("customer_id").sum()
    if dupes:
        rpt.add_issue("duplicate_customer_id", dupes)
    df = df.drop_duplicates("customer_id")

    # Null checks
    null_email = df["email"].isna().sum()
    if null_email:
        rpt.add_issue("null_email", null_email)
    df = df[df["email"].notna()]

    # Email format validation
    email_re = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
    invalid_email = ~df["email"].str.match(email_re)
    rpt.add_issue("invalid_email_format", invalid_email.sum())
    df = df[~invalid_email]

    # Standardize
    df["email"]      = df["email"].str.lower().str.strip()
    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"]  = df["last_name"].str.strip().str.title()
    df["gender"]     = df["gender"].str.upper().str.strip()
    df["country"]    = df["country"].str.strip()
    df["segment"]    = df["segment"].fillna("Unknown")

    # Date
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    future_reg = df["registration_date"] > datetime.now()
    if future_reg.sum():
        rpt.add_issue("future_registration_date", future_reg.sum())
    df = df[~future_reg]

    # DOB age check
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
    too_young = (datetime.now() - df["date_of_birth"]).dt.days < 365 * 16
    if too_young.sum():
        rpt.add_issue("dob_too_young", too_young.sum())
    df = df[~too_young]

    # Add derived
    df["age"] = ((datetime.now() - df["date_of_birth"]).dt.days / 365.25).astype(int)
    df["age_group"] = pd.cut(df["age"],
                             bins=[0,24,34,44,54,150],
                             labels=["18-24","25-34","35-44","45-54","55+"])

    rpt.valid_rows = len(df)
    return df, rpt

# ─── Transform: Products ──────────────────────────────────────────────────────
def transform_products(df: pd.DataFrame) -> tuple[pd.DataFrame, DQReport]:
    rpt = DQReport("products")
    rpt.total_rows = len(df)

    df = df.drop_duplicates("product_id")

    # Price validations
    neg_price = df["unit_price"] <= 0
    rpt.add_issue("non_positive_unit_price", neg_price.sum())
    df = df[~neg_price]

    neg_cost = df["cost_price"] < 0
    rpt.add_issue("negative_cost_price", neg_cost.sum())
    df.loc[neg_cost, "cost_price"] = df.loc[neg_cost, "unit_price"] * 0.5

    # Margin flag
    df["gross_margin_pct"] = (
        (df["unit_price"] - df["cost_price"]) / df["unit_price"] * 100
    ).round(2)

    inverted_margin = df["gross_margin_pct"] < 0
    rpt.add_issue("inverted_margin", inverted_margin.sum())

    # Normalize
    df["product_name"] = df["product_name"].str.strip()
    df["brand"]        = df["brand"].str.strip().str.title()
    df["is_active"]    = df["is_active"].astype(str).str.lower().isin(["true","1","yes"])
    df["launch_date"]  = pd.to_datetime(df["launch_date"], errors="coerce")

    rpt.valid_rows = len(df)
    return df, rpt

# ─── Transform: Orders ────────────────────────────────────────────────────────
def transform_orders(df: pd.DataFrame, df_items: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, DQReport]:
    rpt = DQReport("orders")
    rpt.total_rows = len(df)

    df = df.drop_duplicates("order_id")

    df["order_date"]     = pd.to_datetime(df["order_date"], errors="coerce")
    df["total_amount"]   = pd.to_numeric(df["total_amount"], errors="coerce")
    df["discount_amount"]= pd.to_numeric(df["discount_amount"], errors="coerce").fillna(0)
    df["shipping_cost"]  = pd.to_numeric(df["shipping_cost"], errors="coerce").fillna(0)
    df["tax_amount"]     = pd.to_numeric(df["tax_amount"], errors="coerce").fillna(0)

    neg_total = df["total_amount"] < 0
    rpt.add_issue("negative_total_amount", neg_total.sum())
    df = df[~neg_total]

    valid_statuses = {"COMPLETED","PENDING","SHIPPED","PROCESSING","CANCELLED","REFUNDED"}
    invalid_status = ~df["status"].isin(valid_statuses)
    rpt.add_issue("invalid_status", invalid_status.sum())
    df.loc[invalid_status, "status"] = "PENDING"

    df["channel"]         = df["channel"].fillna("web")
    df["currency"]        = df["currency"].fillna("USD").str.upper()
    df["net_revenue"]     = (df["total_amount"] - df["discount_amount"] - df["shipping_cost"]).round(2)

    # Order items
    df_items["unit_price"]   = pd.to_numeric(df_items["unit_price"], errors="coerce")
    df_items["quantity"]     = pd.to_numeric(df_items["quantity"], errors="coerce").fillna(1).astype(int)
    df_items["discount_pct"] = pd.to_numeric(df_items["discount_pct"], errors="coerce").fillna(0)
    df_items["line_total"]   = pd.to_numeric(df_items["line_total"], errors="coerce")

    # Recalculate line totals for integrity
    df_items["line_total_check"] = (
        df_items["quantity"] * df_items["unit_price"] * (1 - df_items["discount_pct"] / 100)
    ).round(2)

    rpt.valid_rows = len(df)
    return df, df_items, rpt

# ─── Transform: Payments ──────────────────────────────────────────────────────
def transform_payments(df: pd.DataFrame) -> tuple[pd.DataFrame, DQReport]:
    rpt = DQReport("payments")
    rpt.total_rows = len(df)

    df = df.drop_duplicates("payment_id")
    df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")
    df["amount"]       = pd.to_numeric(df["amount"], errors="coerce")

    null_amount = df["amount"].isna()
    rpt.add_issue("null_amount", null_amount.sum())
    df = df[~null_amount]

    valid_pay_methods = {"credit_card","debit_card","paypal","apple_pay",
                         "google_pay","bank_transfer","buy_now_pay_later"}
    df["payment_method"] = df["payment_method"].str.lower().str.strip()
    df.loc[~df["payment_method"].isin(valid_pay_methods), "payment_method"] = "other"

    rpt.valid_rows = len(df)
    return df, rpt

# ─── Transform: Reviews ───────────────────────────────────────────────────────
def transform_reviews(df: pd.DataFrame) -> tuple[pd.DataFrame, DQReport]:
    rpt = DQReport("reviews")
    rpt.total_rows = len(df)

    df = df.drop_duplicates(["customer_id","product_id","review_date"])

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    invalid_rating = ~df["rating"].between(1, 5)
    rpt.add_issue("invalid_rating", invalid_rating.sum())
    df = df[~invalid_rating]
    df["rating"] = df["rating"].astype(int)

    df["review_date"]      = pd.to_datetime(df["review_date"], errors="coerce")
    df["helpful_votes"]    = pd.to_numeric(df["helpful_votes"], errors="coerce").fillna(0).astype(int)
    df["verified_purchase"]= df["verified_purchase"].astype(str).str.lower().isin(["true","1","yes"])
    df["review_body"]      = df["review_body"].str.strip().str[:4000]
    df["review_title"]     = df["review_title"].str.strip().str[:500]

    # Simple sentiment score: rating-based proxy (in real pipeline: NLP)
    df["sentiment_score"]  = (df["rating"] - 3) / 2  # normalised -1 to +1

    rpt.valid_rows = len(df)
    return df, rpt

# ─── Transform: Clickstream ───────────────────────────────────────────────────
def transform_clickstream(df: pd.DataFrame) -> tuple[pd.DataFrame, DQReport]:
    rpt = DQReport("clickstream")
    rpt.total_rows = len(df)

    df = df.drop_duplicates("event_id")
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")

    null_ts = df["event_timestamp"].isna()
    rpt.add_issue("null_timestamp", null_ts.sum())
    df = df[~null_ts]

    valid_events = {"page_view","product_view","add_to_cart","remove_from_cart",
                    "checkout","purchase","search","wishlist_add","review_submit"}
    df["event_type"] = df["event_type"].str.lower().str.strip()
    df.loc[~df["event_type"].isin(valid_events), "event_type"] = "other"

    df["device_type"] = df["device_type"].str.lower().str.strip()
    df["customer_id"] = df["customer_id"].replace("", np.nan)

    rpt.valid_rows = len(df)
    return df, rpt

# ─── Save & Report ─────────────────────────────────────────────────────────────
def save_processed(df: pd.DataFrame, name: str):
    path = PROC_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    log.info("Saved  %-30s  %d rows  →  %s", name, len(df), path)

def save_dq_report(reports: list[DQReport]):
    summary = [r.summary() for r in reports]
    path    = PROC_DIR / "dq_report.json"
    with open(path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    log.info("DQ report saved → %s", path)
    return summary

# ─── Main ETL Orchestration ────────────────────────────────────────────────────
def run_etl():
    log.info("=" * 60)
    log.info("  ETL Transform Pipeline  START")
    log.info("=" * 60)
    reports = []

    def read(name: str) -> pd.DataFrame:
        path = RAW_DIR / f"{name}.csv"
        df = pd.read_csv(path, low_memory=False)
        log.info("Read   %-30s  %d rows", name, len(df))
        return df

    # Customers
    df_cust, rpt = transform_customers(read("customers"))
    save_processed(df_cust, "customers_clean")
    reports.append(rpt)

    # Products
    df_prod, rpt = transform_products(read("products"))
    save_processed(df_prod, "products_clean")
    reports.append(rpt)

    # Orders + Items
    df_ord, df_items, rpt = transform_orders(read("orders"), read("order_items"))
    save_processed(df_ord,   "orders_clean")
    save_processed(df_items, "order_items_clean")
    reports.append(rpt)

    # Payments
    df_pay, rpt = transform_payments(read("payments"))
    save_processed(df_pay, "payments_clean")
    reports.append(rpt)

    # Reviews
    df_rev, rpt = transform_reviews(read("reviews"))
    save_processed(df_rev, "reviews_clean")
    reports.append(rpt)

    # Clickstream
    df_click, rpt = transform_clickstream(read("clickstream"))
    save_processed(df_click, "clickstream_clean")
    reports.append(rpt)

    # DQ Report
    dq = save_dq_report(reports)
    log.info("=" * 60)
    log.info("  ETL Transform Pipeline  COMPLETE")
    log.info("  Tables processed: %d", len(reports))
    total_in  = sum(r.total_rows for r in reports)
    total_out = sum(r.valid_rows for r in reports)
    log.info("  Rows in: %d  →  Rows out: %d  (%.1f%% pass)",
             total_in, total_out, total_out / max(total_in,1) * 100)
    log.info("=" * 60)
    return dq

if __name__ == "__main__":
    run_etl()
