"""
Apache Spark Structured Streaming
E-Commerce Real-Time Clickstream Processing

Streams clickstream events from:
  - Kafka topic: ecom.clickstream.events
  - OR file-based micro-batch (for local testing without Kafka)

Real-time jobs:
  1. Session aggregation (active session count, events/sec)
  2. Real-time funnel tracking (page_view → purchase)
  3. Add-to-cart abandonment detection
  4. Trending products (5-min sliding window)
  5. Fraud / anomaly detection signals
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    TimestampType, IntegerType, BooleanType
)
import logging
from pathlib import Path

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

BASE_DIR   = Path(__file__).parent.parent
RAW_DIR    = str(BASE_DIR / "data" / "raw")
STREAM_OUT = str(BASE_DIR / "data" / "stream_output")
CKPT_DIR   = str(BASE_DIR / "data" / "checkpoints")

# ─── Schema ───────────────────────────────────────────────────────────────────
CLICK_SCHEMA = StructType([
    StructField("event_id",        StringType(),    True),
    StructField("session_id",      StringType(),    True),
    StructField("customer_id",     StringType(),    True),
    StructField("event_type",      StringType(),    True),
    StructField("page_url",        StringType(),    True),
    StructField("product_id",      StringType(),    True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("device_type",     StringType(),    True),
    StructField("browser",         StringType(),    True),
    StructField("referrer",        StringType(),    True),
])

KAFKA_SCHEMA = StructType([
    StructField("event_id",        StringType(),    True),
    StructField("session_id",      StringType(),    True),
    StructField("customer_id",     StringType(),    True),
    StructField("event_type",      StringType(),    True),
    StructField("product_id",      StringType(),    True),
    StructField("event_timestamp", StringType(),    True),
    StructField("device_type",     StringType(),    True),
    StructField("page_url",        StringType(),    True),
])

# ─── Spark Session ────────────────────────────────────────────────────────────
def get_spark(app: str = "EcomStreaming") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", CKPT_DIR)
        .getOrCreate()
    )

# ─── Source: Kafka ────────────────────────────────────────────────────────────
def kafka_source(spark: SparkSession, kafka_servers: str = "localhost:9092") -> "DataFrame":
    """Read from Kafka topic ecom.clickstream.events"""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "ecom.clickstream.events")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10_000)
        .load()
    )
    return (
        raw.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
        .select(F.from_json("json_str", KAFKA_SCHEMA).alias("data"), "kafka_timestamp")
        .select("data.*", "kafka_timestamp")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    )

# ─── Source: File Micro-Batch (Local Testing) ──────────────────────────────────
def file_source(spark: SparkSession) -> "DataFrame":
    """Read from CSV directory as streaming source (for local testing)"""
    return (
        spark.readStream
        .schema(CLICK_SCHEMA)
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
        .csv(RAW_DIR)
        .filter(F.col("event_timestamp").isNotNull())
    )

# ─── JOB A: Real-Time Session Aggregation (1-min tumbling window) ─────────────
def stream_session_agg(stream_df, mode: str = "file"):
    log.info("STREAM JOB A: Session Aggregation")
    session_agg = (
        stream_df
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            F.window("event_timestamp", "1 minute").alias("window"),
            "device_type"
        )
        .agg(
            F.countDistinct("session_id").alias("active_sessions"),
            F.count("event_id").alias("total_events"),
            F.countDistinct("customer_id").alias("unique_users"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "device_type",
            "active_sessions",
            "total_events",
            "unique_users",
        )
    )
    return (
        session_agg.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="30 seconds")
        .queryName("session_aggregation")
        .start()
    )

# ─── JOB B: Trending Products (5-min sliding window, 1-min slide) ─────────────
def stream_trending_products(stream_df):
    log.info("STREAM JOB B: Trending Products")
    trending = (
        stream_df
        .filter(F.col("product_id").isNotNull() & (F.col("product_id") != ""))
        .filter(F.col("event_type").isin("product_view","add_to_cart","purchase"))
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            F.window("event_timestamp", "5 minutes", "1 minute"),
            "product_id",
            "event_type",
        )
        .agg(F.count("event_id").alias("event_count"))
        .withColumn("trend_score",
            F.when(F.col("event_type") == "purchase",     F.col("event_count") * 10)
            .when(F.col("event_type") == "add_to_cart",   F.col("event_count") * 3)
            .otherwise(F.col("event_count"))
        )
    )
    return (
        trending.writeStream
        .outputMode("update")
        .format("parquet")
        .option("path",              f"{STREAM_OUT}/trending_products")
        .option("checkpointLocation", f"{CKPT_DIR}/trending")
        .trigger(processingTime="60 seconds")
        .queryName("trending_products")
        .start()
    )

# ─── JOB C: Abandonment Detection ─────────────────────────────────────────────
def stream_abandonment(stream_df):
    """
    Detect sessions that had add_to_cart but NO purchase in last 10 minutes
    Emits abandonment alerts for remarketing pipeline
    """
    log.info("STREAM JOB C: Cart Abandonment Detection")
    cart_events = (
        stream_df
        .filter(F.col("event_type").isin("add_to_cart","purchase"))
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window("event_timestamp", "10 minutes", "2 minutes"),
            "session_id",
            "customer_id",
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "add_to_cart",  1).otherwise(0)).alias("cart_count"),
            F.sum(F.when(F.col("event_type") == "purchase",     1).otherwise(0)).alias("purchase_count"),
            F.collect_set("product_id").alias("cart_products"),
        )
        .filter(
            (F.col("cart_count") > 0) & (F.col("purchase_count") == 0)
        )
        .withColumn("abandonment_flag", F.lit(True))
    )
    return (
        cart_events.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path",              f"{STREAM_OUT}/abandonment_alerts")
        .option("checkpointLocation", f"{CKPT_DIR}/abandonment")
        .trigger(processingTime="120 seconds")
        .queryName("abandonment_detection")
        .start()
    )

# ─── JOB D: Real-Time Conversion Funnel ───────────────────────────────────────
def stream_funnel(stream_df):
    log.info("STREAM JOB D: Real-Time Conversion Funnel")
    funnel = (
        stream_df
        .withWatermark("event_timestamp", "1 minute")
        .groupBy(
            F.window("event_timestamp", "1 minute"),
        )
        .agg(
            F.countDistinct(F.when(F.col("event_type") == "page_view",   F.col("session_id"))).alias("page_views"),
            F.countDistinct(F.when(F.col("event_type") == "product_view",F.col("session_id"))).alias("product_views"),
            F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("session_id"))).alias("add_to_cart"),
            F.countDistinct(F.when(F.col("event_type") == "checkout",    F.col("session_id"))).alias("checkouts"),
            F.countDistinct(F.when(F.col("event_type") == "purchase",    F.col("session_id"))).alias("purchases"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "page_views","product_views","add_to_cart","checkouts","purchases",
            F.round(F.col("purchases") / F.nullif(F.col("page_views"), F.lit(0)) * 100, 2)
                .alias("conversion_pct")
        )
    )
    return (
        funnel.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="30 seconds")
        .queryName("live_funnel")
        .start()
    )

# ─── JOB E: Anomaly / Fraud Signals ───────────────────────────────────────────
def stream_anomaly(stream_df):
    """Flag sessions with unusually high event velocity (possible bots)"""
    log.info("STREAM JOB E: Anomaly Detection")
    velocity = (
        stream_df
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            "session_id",
            "customer_id",
        )
        .agg(F.count("event_id").alias("event_count"))
        .withColumn("is_anomaly", F.col("event_count") > 200)   # threshold: 200 events/5min
        .filter(F.col("is_anomaly"))
        .withColumn("anomaly_type", F.lit("HIGH_VELOCITY"))
    )
    return (
        velocity.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path",              f"{STREAM_OUT}/anomaly_flags")
        .option("checkpointLocation", f"{CKPT_DIR}/anomaly")
        .trigger(processingTime="60 seconds")
        .queryName("anomaly_detection")
        .start()
    )

# ─── Run All Streaming Jobs ────────────────────────────────────────────────────
def run_streaming(use_kafka: bool = False, kafka_servers: str = "localhost:9092",
                  timeout_ms: int = 120_000):
    spark = get_spark()
    Path(STREAM_OUT).mkdir(parents=True, exist_ok=True)
    Path(CKPT_DIR).mkdir(parents=True, exist_ok=True)

    if use_kafka:
        log.info("Source: Kafka @ %s", kafka_servers)
        stream_df = kafka_source(spark, kafka_servers)
    else:
        log.info("Source: File micro-batch @ %s", RAW_DIR)
        stream_df = file_source(spark)

    queries = [
        stream_session_agg(stream_df),
        stream_trending_products(stream_df),
        stream_abandonment(stream_df),
        stream_funnel(stream_df),
        stream_anomaly(stream_df),
    ]

    log.info("Started %d streaming queries. Waiting %d ms …", len(queries), timeout_ms)
    for q in queries:
        q.awaitTermination(timeout=timeout_ms // 1000)

    log.info("Streaming run complete.")
    spark.stop()

if __name__ == "__main__":
    run_streaming(use_kafka=False, timeout_ms=60_000)
