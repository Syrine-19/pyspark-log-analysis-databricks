"""
anomaly_detection.py
--------------------
Rule-based and statistical anomaly detection on log data.

Detects four classes of production anomalies:
    1. HIGH_LATENCY   — response spikes above dynamic threshold
    2. ERROR_SPIKE    — sudden burst of 5xx in a time window
    3. NULL_USER_ID   — rows without a user_id (bot / crawler traffic)
    4. BRUTE_FORCE    — single IP generating repeated 401 failures
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import logging

logger = logging.getLogger(__name__)


# ── Thresholds (tune per environment) ────────────────────────────────────────

LATENCY_MULTIPLIER    = 3.0    # flag if response_time > mean + N * stddev
ERROR_SPIKE_THRESHOLD = 10     # % error rate in a window triggers alert
BRUTE_FORCE_MIN_REQS  = 50     # min requests from one IP in 5 min window
BRUTE_FORCE_FAIL_RATE = 0.8    # 80 % of those must be 401/403


# ── 1. High-latency detection (z-score per service) ──────────────────────────

def detect_high_latency(df: DataFrame) -> DataFrame:
    """
    Flags rows where response_time_ms > mean + LATENCY_MULTIPLIER * stddev
    computed per service.  Uses a window function — no full shuffle needed.
    """
    w = Window.partitionBy("service")

    df = (
        df
        .withColumn("_mean_rt", F.avg("response_time_ms").over(w))
        .withColumn("_std_rt",  F.stddev("response_time_ms").over(w))
        .withColumn(
            "is_high_latency",
            F.col("response_time_ms") > (
                F.col("_mean_rt") + LATENCY_MULTIPLIER * F.col("_std_rt")
            ),
        )
        .drop("_mean_rt", "_std_rt")
    )
    count = df.filter("is_high_latency").count()
    print(f"[anomaly] HIGH_LATENCY — {count:,} rows flagged")
    logger.info("HIGH_LATENCY: %d rows", count)
    return df


# ── 2. Error spike detection (5-minute tumbling window) ───────────────────────

def detect_error_spikes(df: DataFrame) -> DataFrame:
    """
    Groups requests into 5-minute windows per service and flags windows
    where the error rate exceeds ERROR_SPIKE_THRESHOLD %.
    Returns a summary DataFrame (not row-level).
    """
    agg = (
        df
        .withColumn("window", F.window("timestamp", "5 minutes"))
        .groupBy("service", "window")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.col("is_error").cast("int")).alias("errors"),
        )
        .withColumn(
            "error_rate_pct",
            F.round(F.col("errors") / F.col("total") * 100, 2),
        )
        .withColumn(
            "is_error_spike",
            F.col("error_rate_pct") >= ERROR_SPIKE_THRESHOLD,
        )
        .filter("is_error_spike")
        .orderBy(F.col("error_rate_pct").desc())
    )

    spike_count = agg.count()
    print(f"[anomaly] ERROR_SPIKE — {spike_count:,} windows above {ERROR_SPIKE_THRESHOLD}% error rate")
    logger.info("ERROR_SPIKE: %d windows", spike_count)
    return agg


# ── 3. Null user_id detection (bot / unauthenticated traffic) ─────────────────

def detect_null_user_ids(df: DataFrame) -> DataFrame:
    """
    Extracts rows with a null user_id — indicative of scrapers or bots
    that bypass authentication.
    """
    df_nulls = df.filter(F.col("user_id").isNull())
    count    = df_nulls.count()
    print(f"[anomaly] NULL_USER_ID — {count:,} rows with missing user_id")
    logger.info("NULL_USER_ID: %d rows", count)
    return df_nulls


# ── 4. Brute-force / DDoS detection (IP + 5-min window) ───────────────────────

def detect_brute_force(df: DataFrame) -> DataFrame:
    """
    Finds IPs making >= BRUTE_FORCE_MIN_REQS requests in any 5-minute window
    with >= BRUTE_FORCE_FAIL_RATE fraction being 401/403 responses.
    Classic brute-force / credential-stuffing signature.
    """
    agg = (
        df
        .withColumn("window", F.window("timestamp", "5 minutes"))
        .groupBy("ip_address", "window")
        .agg(
            F.count("*").alias("total"),
            F.sum(
                F.when(F.col("status_code").isin(401, 403), 1).otherwise(0)
            ).alias("auth_failures"),
        )
        .withColumn(
            "fail_rate",
            F.round(F.col("auth_failures") / F.col("total"), 3),
        )
        .filter(
            (F.col("total") >= BRUTE_FORCE_MIN_REQS)
            & (F.col("fail_rate") >= BRUTE_FORCE_FAIL_RATE)
        )
        .withColumn("anomaly_type", F.lit("BRUTE_FORCE"))
        .orderBy(F.col("auth_failures").desc())
    )

    count = agg.count()
    print(f"[anomaly] BRUTE_FORCE — {count:,} suspicious IP/window pairs")
    logger.info("BRUTE_FORCE: %d IP-window pairs", count)
    return agg


# ── 5. Unified anomaly report ─────────────────────────────────────────────────

def build_anomaly_report(df: DataFrame) -> DataFrame:
    """
    Consolidates all row-level anomaly flags into a single report DataFrame.

    Columns:
        timestamp, service, ip_address, status_code, response_time_ms,
        anomaly_flags  — comma-separated list of detected anomalies
        anomaly_count  — number of anomaly types on this row
    """
    df = detect_high_latency(df)

    df = (
        df
        .withColumn("null_uid_flag", F.col("user_id").isNull())
    )

    # Build a list of active flags per row
    flag_col = F.concat_ws(
        ",",
        F.when(F.col("is_error"),        F.lit("ERROR")).otherwise(F.lit("")),
        F.when(F.col("is_high_latency"), F.lit("HIGH_LATENCY")).otherwise(F.lit("")),
        F.when(F.col("is_slow"),         F.lit("SLOW")).otherwise(F.lit("")),
        F.when(F.col("null_uid_flag"),   F.lit("NULL_USER_ID")).otherwise(F.lit("")),
    )

    df = (
        df
        .withColumn("anomaly_flags", flag_col)
        .withColumn(
            "anomaly_count",
            F.size(F.array_remove(F.split("anomaly_flags", ","), "")),
        )
    )

    report = (
        df
        .filter(F.col("anomaly_count") > 0)
        .select(
            "timestamp", "service", "endpoint",
            "ip_address", "status_code", "response_time_ms",
            "anomaly_flags", "anomaly_count",
        )
        .orderBy(F.col("anomaly_count").desc(), "timestamp")
    )

    total = report.count()
    print(f"[anomaly] Unified report — {total:,} anomalous rows identified")
    return report


# ── Public API ────────────────────────────────────────────────────────────────

def run_anomaly_detection(df: DataFrame) -> dict[str, DataFrame]:
    """
    Full anomaly detection pipeline.

    Returns:
        {
            "report":       consolidated row-level anomaly report,
            "error_spikes": 5-min window error spike summary,
            "brute_force":  IP-level brute-force summary,
            "null_users":   rows with missing user_id,
        }
    """
    print("[anomaly] Starting anomaly detection pipeline...")
    return {
        "report":       build_anomaly_report(df),
        "error_spikes": detect_error_spikes(df),
        "brute_force":  detect_brute_force(df),
        "null_users":   detect_null_user_ids(df),
    }
