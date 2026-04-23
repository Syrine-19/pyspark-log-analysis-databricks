"""
test_processing.py
------------------
Unit tests for the processing and anomaly detection modules.
Uses a small in-memory DataFrame (no CSV required).

Run:
    pytest tests/ -v
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, TimestampType,
)

# ── Fixture: local SparkSession ───────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("TestLogAnalysis")
        .config("spark.sql.shuffle.partitions", "4")   # small — tests only
        .config("spark.sql.adaptive.enabled", "false") # deterministic for tests
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Fixture: sample DataFrame ─────────────────────────────────────────────────

SCHEMA = StructType([
    StructField("timestamp",        TimestampType(), True),
    StructField("log_level",        StringType(),    True),
    StructField("service",          StringType(),    True),
    StructField("http_method",      StringType(),    True),
    StructField("endpoint",         StringType(),    True),
    StructField("status_code",      IntegerType(),   True),
    StructField("response_time_ms", IntegerType(),   True),
    StructField("bytes_sent",       LongType(),      True),
    StructField("user_id",          LongType(),      True),
    StructField("ip_address",       StringType(),    True),
    StructField("user_agent",       StringType(),    True),
    StructField("anomaly_type",     StringType(),    True),
])

SAMPLE_ROWS = [
    (datetime(2024, 1, 15, 9, 0, 0),  "INFO",    "auth-api",    "GET",  "/api/auth/login",  200, 120,   1024, 1001, "10.0.0.1", "Mozilla/5.0", "NONE"),
    (datetime(2024, 1, 15, 9, 0, 5),  "ERROR",   "auth-api",    "POST", "/api/auth/login",  500, 3500,  512,  1002, "10.0.0.2", "Mozilla/5.0", "HIGH_LATENCY"),
    (datetime(2024, 1, 15, 9, 0, 10), "INFO",    "product-svc", "GET",  "/api/products",    200, 80,    2048, 1003, "10.0.0.3", "curl/7.85",   "NONE"),
    (datetime(2024, 1, 15, 9, 0, 15), "WARNING", "auth-api",    "POST", "/api/auth/login",  401, 95,    256,  None, "10.0.0.2", "bot/1.0",     "NULL_USER_ID"),
    (datetime(2024, 1, 15, 9, 0, 20), "ERROR",   "order-svc",   "POST", "/api/orders",      503, 12000, 128,  1004, "10.0.0.4", "Mozilla/5.0", "HIGH_LATENCY"),
    (datetime(2024, 1, 15, 9, 0, 25), "INFO",    "product-svc", "GET",  "/api/products",    200, 65,    3072, 1005, "10.0.0.5", "Mozilla/5.0", "NONE"),
    (datetime(2024, 1, 15, 9, 0, 30), "ERROR",   "auth-api",    "GET",  "/api/auth/login",  502, 8000,  256,  None, "10.0.0.2", "bot/1.0",     "NONE"),
    (datetime(2024, 1, 15, 9, 0, 35), "INFO",    "search-svc",  "GET",  "/api/search",      200, 300,   4096, 1006, "10.0.0.6", "Mozilla/5.0", "NONE"),
]


@pytest.fixture(scope="session")
def sample_df(spark):
    return spark.createDataFrame(SAMPLE_ROWS, schema=SCHEMA)


# ── Import modules under test ─────────────────────────────────────────────────

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from processing        import enrich, aggregate_by_service, top_slow_endpoints
from anomaly_detection import detect_high_latency, detect_null_user_ids


# ── Tests: enrichment ─────────────────────────────────────────────────────────

class TestEnrich:
    def test_adds_date_column(self, sample_df):
        df = enrich(sample_df)
        assert "date" in df.columns

    def test_adds_hour_column(self, sample_df):
        df = enrich(sample_df)
        assert "hour" in df.columns

    def test_adds_is_error_column(self, sample_df):
        df = enrich(sample_df)
        assert "is_error" in df.columns

    def test_is_error_flags_5xx(self, sample_df):
        df = enrich(sample_df)
        errors = df.filter("status_code >= 500 AND NOT is_error").count()
        assert errors == 0, "All 5xx rows must be flagged as is_error=True"

    def test_is_error_does_not_flag_2xx(self, sample_df):
        df = enrich(sample_df)
        false_positives = df.filter("status_code < 400 AND is_error").count()
        assert false_positives == 0

    def test_status_class_values(self, sample_df):
        df = enrich(sample_df)
        classes = {r["status_class"] for r in df.select("status_class").distinct().collect()}
        assert classes.issubset({"2xx", "3xx", "4xx", "5xx"})

    def test_is_slow_threshold(self, sample_df):
        df = enrich(sample_df)
        # Row with response_time_ms=12000 must be slow
        slow = df.filter("response_time_ms > 2000 AND NOT is_slow").count()
        assert slow == 0


# ── Tests: aggregation ────────────────────────────────────────────────────────

class TestAggregation:
    def test_aggregate_by_service_columns(self, sample_df):
        df = enrich(sample_df)
        agg = aggregate_by_service(df)
        expected = {"service", "date", "total_requests", "error_count", "error_rate_pct",
                    "p50_ms", "p95_ms", "p99_ms", "total_bytes_sent"}
        assert expected.issubset(set(agg.columns))

    def test_aggregate_row_count(self, sample_df):
        df = enrich(sample_df)
        agg = aggregate_by_service(df)
        # 3 distinct services × 1 date = 3 rows
        assert agg.count() == 3

    def test_total_requests_sum(self, sample_df):
        df = enrich(sample_df)
        agg = aggregate_by_service(df)
        total = agg.agg({"total_requests": "sum"}).collect()[0][0]
        assert total == len(SAMPLE_ROWS)

    def test_top_slow_endpoints_limit(self, sample_df):
        df = enrich(sample_df)
        slow = top_slow_endpoints(df, n=3)
        assert slow.count() <= 3


# ── Tests: anomaly detection ──────────────────────────────────────────────────

class TestAnomalyDetection:
    def test_detect_high_latency_adds_column(self, sample_df):
        df = enrich(sample_df)
        df = detect_high_latency(df)
        assert "is_high_latency" in df.columns

    def test_high_latency_flags_outliers(self, sample_df):
        df = enrich(sample_df)
        df = detect_high_latency(df)
        # Rows with 12000ms and 8000ms should be flagged
        flagged = df.filter("is_high_latency").count()
        assert flagged >= 1

    def test_detect_null_user_ids(self, sample_df):
        nulls = detect_null_user_ids(sample_df)
        # SAMPLE_ROWS has 2 rows with user_id=None
        assert nulls.count() == 2

    def test_null_user_id_all_have_none(self, sample_df):
        nulls = detect_null_user_ids(sample_df)
        non_null = nulls.filter("user_id IS NOT NULL").count()
        assert non_null == 0


# ── Tests: schema / data quality ─────────────────────────────────────────────

class TestDataQuality:
    def test_no_negative_response_times(self, sample_df):
        bad = sample_df.filter("response_time_ms < 0").count()
        assert bad == 0

    def test_status_codes_in_valid_range(self, sample_df):
        bad = sample_df.filter("status_code < 100 OR status_code >= 600").count()
        assert bad == 0

    def test_row_count(self, sample_df):
        assert sample_df.count() == len(SAMPLE_ROWS)
