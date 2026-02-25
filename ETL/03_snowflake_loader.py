"""
E-Commerce ETL Pipeline - Snowflake Loader
Uploads processed CSV files to Snowflake internal stages
and executes COPY INTO + MERGE operations
"""

import os
import logging
from pathlib import Path

log = logging.getLogger("snowflake_loader")

# ─── Snowflake connection (requires snowflake-connector-python) ──────────────
try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    HAS_SNOWFLAKE = True
except ImportError:
    HAS_SNOWFLAKE = False
    log.warning("snowflake-connector-python not installed. Running in dry-run mode.")

PROC_DIR = Path(__file__).parent.parent / "data" / "processed"

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT",   "your_account"),
    "user":      os.getenv("SNOWFLAKE_USER",       "your_user"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD",   "your_password"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE",  "ETL_WH"),
    "database":  os.getenv("SNOWFLAKE_DATABASE",   "ECOM_DW"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",      "STAGE"),
    "role":      os.getenv("SNOWFLAKE_ROLE",        "DATA_ENGINEER"),
}

# ─── Stage → Table mapping ────────────────────────────────────────────────────
LOAD_MANIFEST = [
    {
        "file":         "customers_clean.csv",
        "stage":        "@stg_customers_stage",
        "table":        "STG_CUSTOMERS",
        "merge_script": "01_stage_to_curated.sql",
    },
    {
        "file":   "products_clean.csv",
        "stage":  "@stg_products_stage",
        "table":  "STG_PRODUCTS",
    },
    {
        "file":   "orders_clean.csv",
        "stage":  "@stg_orders_stage",
        "table":  "STG_ORDERS",
    },
    {
        "file":   "order_items_clean.csv",
        "stage":  "@stg_orders_stage",
        "table":  "STG_ORDER_ITEMS",
    },
    {
        "file":   "payments_clean.csv",
        "stage":  "@stg_payments_stage",
        "table":  "STG_PAYMENTS",
    },
    {
        "file":   "reviews_clean.csv",
        "stage":  "@stg_reviews_stage",
        "table":  "STG_REVIEWS",
    },
]

COPY_INTO_TEMPLATE = """
COPY INTO {table}
FROM {stage}/{file}
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE'
PURGE = FALSE
FORCE = FALSE;
"""

class SnowflakeLoader:
    def __init__(self):
        self.conn = None
        self.cur  = None

    def connect(self):
        if not HAS_SNOWFLAKE:
            log.info("[DRY-RUN] Would connect to Snowflake: %s", SNOWFLAKE_CONFIG["account"])
            return
        log.info("Connecting to Snowflake account: %s", SNOWFLAKE_CONFIG["account"])
        self.conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        self.cur  = self.conn.cursor()
        log.info("Connected.")

    def execute(self, sql: str, description: str = ""):
        if not HAS_SNOWFLAKE:
            log.info("[DRY-RUN] SQL: %s", description or sql[:80])
            return []
        log.info("Executing: %s", description or sql[:80])
        self.cur.execute(sql)
        return self.cur.fetchall()

    def put_file(self, local_path: Path, stage: str):
        """Upload local file to Snowflake internal stage"""
        if not HAS_SNOWFLAKE:
            log.info("[DRY-RUN] PUT %s → %s", local_path, stage)
            return
        put_sql = f"PUT file://{local_path} {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
        log.info("PUT %s → %s", local_path.name, stage)
        self.cur.execute(put_sql)
        result = self.cur.fetchone()
        log.info("PUT result: %s", result)

    def copy_into(self, table: str, stage: str, file: str):
        sql = COPY_INTO_TEMPLATE.format(table=table, stage=stage, file=file)
        rows = self.execute(sql, f"COPY INTO {table}")
        log.info("COPY INTO %s complete", table)
        return rows

    def run_sql_file(self, sql_file: Path):
        with open(sql_file) as f:
            content = f.read()
        statements = [s.strip() for s in content.split(";") if s.strip()]
        for stmt in statements:
            if stmt.startswith("--"):
                continue
            self.execute(stmt)

    def load_all(self):
        self.connect()
        log.info("=" * 55)
        log.info("  Snowflake Load  START")
        log.info("=" * 55)

        for item in LOAD_MANIFEST:
            local = PROC_DIR / item["file"]
            if not local.exists():
                log.warning("File not found: %s — skipping", local)
                continue

            # Step 1: PUT to stage
            self.put_file(local, item["stage"])

            # Step 2: COPY INTO stage table
            self.copy_into(item["table"], item["stage"], item["file"])

        # Step 3: Run Stage → Curated MERGE
        log.info("Running Stage → Curated transformation …")
        merge_path = Path(__file__).parent.parent / "sql" / "dml" / "01_stage_to_curated.sql"
        if merge_path.exists():
            self.run_sql_file(merge_path)
            log.info("Stage → Curated MERGE complete")

        log.info("=" * 55)
        log.info("  Snowflake Load  COMPLETE")
        log.info("=" * 55)

    def close(self):
        if self.conn:
            self.conn.close()
            log.info("Snowflake connection closed.")

# ─── Orchestrate Full Pipeline ─────────────────────────────────────────────────
def run_full_pipeline():
    """
    Run complete ETL pipeline:
    1. Generate raw data       (01_generate_data.py)
    2. Transform & validate    (02_transform_load.py)
    3. Load to Snowflake       (this module)
    """
    import sys, importlib
    sys.path.insert(0, str(Path(__file__).parent))

    # Step 1: Generate
    log.info("Step 1/3 — Generate raw data")
    gen = importlib.import_module("01_generate_data")
    gen.main()

    # Step 2: Transform
    log.info("Step 2/3 — Transform & validate")
    tl = importlib.import_module("02_transform_load")
    tl.run_etl()

    # Step 3: Load
    log.info("Step 3/3 — Load to Snowflake")
    loader = SnowflakeLoader()
    try:
        loader.load_all()
    finally:
        loader.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(message)s")
    run_full_pipeline()
