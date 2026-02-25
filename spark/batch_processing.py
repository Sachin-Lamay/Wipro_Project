"""
Apache Spark Batch Processing
E-Commerce Customer Behavior & Recommendation System

Jobs:
  1. Ingest CSVs → Parquet (raw zone)
  2. Compute RFM scores
  3. Product affinity (co-purchase matrix)
  4. Compute daily KPI aggregations
  5. Write results to processed zone & Snowflake
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, TimestampType, DateType
)
import logging, os
from pathlib import Path
from datetime import date

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

BASE_DIR  = Path(__file__).parent.parent
RAW_DIR   = str(BASE_DIR / "data" / "raw")
PROC_DIR  = str(BASE_DIR / "data" / "processed")
PARQ_DIR  = str(BASE_DIR / "data" / "parquet")

# ─── Spark Session ────────────────────────────────────────────────────────────
def get_spark(app_name: str = "EcomBatch") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Snowflake connector (optional)
        # .config("spark.jars", "snowflake-jdbc-*.jar,spark-snowflake_*.jar")
        .getOrCreate()
    )

# ─── JOB 1: CSV → Parquet ─────────────────────────────────────────────────────
def job_ingest_to_parquet(spark: SparkSession):
    log.info("JOB 1: Ingest CSV → Parquet")

    tables = ["customers", "products", "orders", "order_items",
              "payments", "reviews", "clickstream"]

    for t in tables:
        csv_path  = f"{RAW_DIR}/{t}.csv"
        parq_path = f"{PARQ_DIR}/{t}"
        if not Path(csv_path).exists():
            log.warning("CSV not found: %s — skip", csv_path)
            continue
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        df.write.mode("overwrite").parquet(parq_path)
        log.info("  %s  →  %s  (%d rows)", t, parq_path, df.count())

# ─── JOB 2: RFM Scoring ───────────────────────────────────────────────────────
def job_rfm(spark: SparkSession) -> DataFrame:
    log.info("JOB 2: RFM Scoring")

    orders = spark.read.parquet(f"{PARQ_DIR}/orders").filter(
        F.col("status").isin("COMPLETED","SHIPPED")
    )

    today = F.lit(str(date.today())).cast(DateType())

    rfm_base = (
        orders
        .withColumn("order_date", F.to_timestamp("order_date"))
        .groupBy("customer_id")
        .agg(
            F.datediff(today, F.max(F.to_date("order_date")).cast(DateType())).alias("recency"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("net_revenue"), 2).alias("monetary"),
            F.min(F.to_date("order_date")).alias("first_order_date"),
            F.max(F.to_date("order_date")).alias("last_order_date"),
        )
    )

    # Quantile scoring using approxQuantile-based bucketing
    r_buckets = rfm_base.approxQuantile("recency",   [0.2, 0.4, 0.6, 0.8], 0.05)
    f_buckets = rfm_base.approxQuantile("frequency", [0.2, 0.4, 0.6, 0.8], 0.05)
    m_buckets = rfm_base.approxQuantile("monetary",  [0.2, 0.4, 0.6, 0.8], 0.05)

    def make_score_col(col_name: str, buckets: list, ascending: bool = True) -> F.Column:
        c = F.col(col_name)
        if ascending:
            return (
                F.when(c <= buckets[0], 5)
                .when(c <= buckets[1], 4)
                .when(c <= buckets[2], 3)
                .when(c <= buckets[3], 2)
                .otherwise(1)
            )
        else:
            return (
                F.when(c >= buckets[3], 5)
                .when(c >= buckets[2], 4)
                .when(c >= buckets[1], 3)
                .when(c >= buckets[0], 2)
                .otherwise(1)
            )

    rfm = (
        rfm_base
        .withColumn("r_score", make_score_col("recency",   r_buckets, ascending=True))
        .withColumn("f_score", make_score_col("frequency", f_buckets, ascending=False))
        .withColumn("m_score", make_score_col("monetary",  m_buckets, ascending=False))
        .withColumn("rfm_score", F.concat(F.col("r_score"), F.col("f_score"), F.col("m_score")))
        .withColumn("rfm_segment",
            F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "Champions")
            .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3),                             "Loyal Customers")
            .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2),                             "Promising")
            .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3), "At Risk")
            .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2) & (F.col("m_score") <= 2), "Lost")
            .when((F.col("r_score") >= 3) & (F.col("m_score") >= 4),                             "Big Spenders")
            .otherwise("Needs Attention")
        )
    )

    rfm.write.mode("overwrite").parquet(f"{PROC_DIR}/rfm_scores")
    log.info("  RFM scores written → %s/rfm_scores", PROC_DIR)
    return rfm

# ─── JOB 3: Product Affinity (Market Basket) ─────────────────────────────────
def job_product_affinity(spark: SparkSession) -> DataFrame:
    log.info("JOB 3: Product Affinity (co-purchase)")

    items = spark.read.parquet(f"{PARQ_DIR}/order_items").select("order_id","product_id")
    total_orders = items.select("order_id").distinct().count()

    # Self-join to create product pairs
    pairs = (
        items.alias("a")
        .join(items.alias("b"), on="order_id")
        .filter(F.col("a.product_id") < F.col("b.product_id"))   # avoid duplicates
        .groupBy(
            F.col("a.product_id").alias("product_a"),
            F.col("b.product_id").alias("product_b"),
        )
        .agg(F.countDistinct("order_id").alias("co_purchase_count"))
        .filter(F.col("co_purchase_count") >= 3)   # min support threshold
    )

    # Support and confidence
    support_a = items.groupBy("product_id").agg(
        F.countDistinct("order_id").alias("orders_a")
    ).withColumnRenamed("product_id","product_a")
    support_b = items.groupBy("product_id").agg(
        F.countDistinct("order_id").alias("orders_b")
    ).withColumnRenamed("product_id","product_b")

    affinity = (
        pairs
        .join(support_a, on="product_a")
        .join(support_b, on="product_b")
        .withColumn("support",    F.round(F.col("co_purchase_count") / total_orders, 6))
        .withColumn("confidence", F.round(F.col("co_purchase_count") / F.col("orders_a"), 4))
        .withColumn("lift",       F.round(
            (F.col("co_purchase_count") / total_orders) /
            ((F.col("orders_a") / total_orders) * (F.col("orders_b") / total_orders)),
        4))
        .select("product_a","product_b","co_purchase_count","support","confidence","lift")
        .filter(F.col("lift") > 1.0)
        .orderBy(F.col("lift").desc())
    )

    affinity.write.mode("overwrite").parquet(f"{PROC_DIR}/product_affinity")
    log.info("  Affinity pairs written → %s/product_affinity", PROC_DIR)
    return affinity

# ─── JOB 4: Daily KPI Aggregations ───────────────────────────────────────────
def job_daily_kpis(spark: SparkSession) -> DataFrame:
    log.info("JOB 4: Daily KPI Aggregations")

    orders = (
        spark.read.parquet(f"{PARQ_DIR}/orders")
        .filter(F.col("status").isin("COMPLETED","SHIPPED"))
        .withColumn("order_date", F.to_date("order_date"))
    )

    daily = (
        orders.groupBy("order_date", "channel")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("total_amount"),2).alias("gross_revenue"),
            F.round(F.sum("discount_amount"),2).alias("total_discounts"),
            F.round(F.sum("tax_amount"),2).alias("total_tax"),
            F.round(F.sum("net_revenue"),2).alias("net_revenue"),
            F.round(F.avg("net_revenue"),2).alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    )

    # MoM growth via window
    w = Window.partitionBy("channel").orderBy("order_date")
    daily_with_lag = (
        daily
        .withColumn("prev_net_revenue", F.lag("net_revenue", 1).over(w))
        .withColumn("mom_growth_pct", F.round(
            (F.col("net_revenue") - F.col("prev_net_revenue")) /
            F.nullif(F.col("prev_net_revenue"), F.lit(0)) * 100, 2
        ))
    )

    daily_with_lag.write.mode("overwrite").partitionBy("order_date").parquet(f"{PROC_DIR}/daily_kpis")
    log.info("  Daily KPIs written → %s/daily_kpis", PROC_DIR)
    return daily_with_lag

# ─── JOB 5: CLV Calculation ───────────────────────────────────────────────────
def job_clv(spark: SparkSession) -> DataFrame:
    log.info("JOB 5: CLV Calculation")

    orders = spark.read.parquet(f"{PARQ_DIR}/orders").filter(
        F.col("status").isin("COMPLETED","SHIPPED")
    )

    today = F.lit(str(date.today())).cast(DateType())

    clv = (
        orders
        .withColumn("order_date", F.to_date("order_date"))
        .groupBy("customer_id")
        .agg(
            F.sum("net_revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("total_orders"),
            F.avg("net_revenue").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn("lifespan_days",
            F.datediff(F.col("last_order_date"), F.col("first_order_date")) + 1)
        .withColumn("months_active",
            F.greatest(F.col("lifespan_days") / 30.0, F.lit(1.0)))
        .withColumn("purchase_freq_monthly",
            F.round(F.col("total_orders") / F.col("months_active"), 4))
        .withColumn("historical_clv",  F.round(F.col("total_revenue"), 2))
        .withColumn("predicted_clv_90d",
            F.round(F.col("avg_order_value") * F.col("purchase_freq_monthly") * 3 * 0.65, 2))
        .withColumn("predicted_clv_365d",
            F.round(F.col("avg_order_value") * F.col("purchase_freq_monthly") * 12 * 0.65, 2))
        .withColumn("clv_tier",
            F.when(F.col("predicted_clv_365d") >= 5000, "Platinum")
            .when(F.col("predicted_clv_365d") >= 2000, "Gold")
            .when(F.col("predicted_clv_365d") >= 500,  "Silver")
            .otherwise("Bronze")
        )
    )

    clv.write.mode("overwrite").parquet(f"{PROC_DIR}/clv_scores")
    log.info("  CLV scores written → %s/clv_scores", PROC_DIR)
    return clv

# ─── Orchestrate ──────────────────────────────────────────────────────────────
def run_batch():
    spark = get_spark("EcomBatch")
    try:
        job_ingest_to_parquet(spark)
        rfm      = job_rfm(spark)
        affinity = job_product_affinity(spark)
        kpis     = job_daily_kpis(spark)
        clv      = job_clv(spark)

        log.info("=" * 55)
        log.info("  Spark Batch Jobs COMPLETE")
        log.info("  RFM segments: %s", rfm.groupBy("rfm_segment").count().collect())
        log.info("  Affinity pairs: %d", affinity.count())
        log.info("  CLV tiers: %s", clv.groupBy("clv_tier").count().collect())
        log.info("=" * 55)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_batch()
