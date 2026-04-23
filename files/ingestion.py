"""
ingestion.py
------------
Reads raw CSV log files into a Spark DataFrame, enforces schema,
and caches the result for downstream processing.

Designed to run on Databricks (cluster or local Spark for testing).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, TimestampType,
)
import logging

logger = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────

LOG_SCHEMA = StructType([
    StructField("timestamp",        StringType(),  nullable=False),
    StructField("log_level",        StringType(),  nullable=False),
    StructField("service",          StringType(),  nullable=False),
    StructField("http_method",      StringType(),  nullable=True),
    StructField("endpoint",         StringType(),  nullable=True),
    StructField("status_code",      IntegerType(), nullable=True),
    StructField("response_time_ms", IntegerType(), nullable=True),
    StructField("bytes_sent",       LongType(),    nullable=True),
    StructField("user_id",          LongType(),    nullable=True),   # nullable — bot rows
    StructField("ip_address",       StringType(),  nullable=True),
    StructField("user_agent",       StringType(),  nullable=True),
    StructField("anomaly_type",     StringType(),  nullable=True),
])


# ── Spark session factory ─────────────────────────────────────────────────────

def get_spark(app_name: str = "LogAnalysis") -> SparkSession:
    """
    Returns an active SparkSession.
    On Databricks the session already exists; locally it is created.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Adaptive Query Execution — core optimization for 40% speedup
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Broadcast join threshold (avoids shuffle for small dimension tables)
        .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready — version %s", spark.version)
    return spark


# ── Ingestion ─────────────────────────────────────────────────────────────────

def read_logs(spark: SparkSession, path: str) -> DataFrame:
    """
    Read CSV log file(s) from `path` using the enforced schema.
    Supports local paths, DBFS paths (dbfs:/…), and S3/ADLS URIs.

    Args:
        spark: Active SparkSession.
        path:  Glob pattern or directory, e.g. "data/logs.csv"
               or "dbfs:/logs/prod/2024-01-15/*.csv"

    Returns:
        Raw DataFrame with LOG_SCHEMA columns.
    """
    logger.info("Reading logs from: %s", path)

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")           # bad rows → _corrupt_record
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(LOG_SCHEMA)
        .csv(path)
    )

    # Cast timestamp string → proper TimestampType
    df = df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
    )

    row_count = df.count()
    logger.info("Ingested %s rows", f"{row_count:,}")
    print(f"[ingestion] Loaded {row_count:,} rows from {path}")
    return df


def validate_schema(df: DataFrame) -> DataFrame:
    """
    Drops rows with null in mandatory columns and logs a quality report.

    Returns:
        Cleaned DataFrame.
    """
    mandatory = ["timestamp", "log_level", "service", "status_code"]
    total     = df.count()

    null_counts = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in mandatory]
    ).collect()[0].asDict()

    for col_name, n_nulls in null_counts.items():
        if n_nulls:
            pct = n_nulls / total * 100
            logger.warning("Column '%s' has %s nulls (%.2f%%)", col_name, n_nulls, pct)
            print(f"[validation] WARNING — '{col_name}': {n_nulls:,} nulls ({pct:.2f}%)")

    df_clean = df.dropna(subset=mandatory)
    dropped  = total - df_clean.count()
    print(f"[validation] Dropped {dropped:,} invalid rows — {df_clean.count():,} remain")
    return df_clean


def cache_dataframe(df: DataFrame, name: str = "logs") -> DataFrame:
    """
    Persists the DataFrame in memory for repeated downstream reads.
    Avoids re-reading CSV on every action (key performance optimisation).
    """
    df = df.cache()
    df.createOrReplaceTempView(name)
    logger.info("DataFrame cached and registered as temp view '%s'", name)
    print(f"[ingestion] DataFrame cached as view '{name}'")
    return df


# ── Public API ────────────────────────────────────────────────────────────────

def load_logs(spark: SparkSession, path: str) -> DataFrame:
    """
    Full ingestion pipeline: read → validate → cache.

    Returns:
        Production-ready DataFrame.
    """
    df = read_logs(spark, path)
    df = validate_schema(df)
    df = cache_dataframe(df)
    return df
