"""
generate_logs.py
----------------
Generates synthetic server log data (1M+ rows) in CSV format.
Simulates realistic patterns including anomalies for detection testing.

Usage:
    python data/generate_logs.py --rows 1000000 --output data/logs.csv
"""

import csv
import random
import argparse
from datetime import datetime, timedelta

# ── Constants ────────────────────────────────────────────────────────────────

HTTP_METHODS  = ["GET", "POST", "PUT", "DELETE", "PATCH"]
ENDPOINTS     = [
    "/api/users", "/api/auth/login", "/api/auth/logout",
    "/api/products", "/api/orders", "/api/payments",
    "/api/search", "/api/recommendations", "/health",
]
STATUS_NORMAL = [200, 201, 204, 301, 304]
STATUS_WARN   = [400, 401, 403, 404]
STATUS_ERROR  = [500, 502, 503, 504]
SERVICES      = ["auth-api", "product-svc", "order-svc", "payment-svc", "search-svc"]
LOG_LEVELS    = ["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"]
USER_AGENTS   = [
    "Mozilla/5.0 (Windows NT 10.0)", "Mozilla/5.0 (Macintosh)",
    "python-requests/2.28", "curl/7.85.0", "bot/1.0",
]

# Anomaly scenarios injected into the data
ANOMALY_SCENARIOS = [
    "HIGH_LATENCY",       # response_time_ms > 5000
    "ERROR_SPIKE",        # burst of 5xx in short window
    "NULL_USER_ID",       # missing user_id (bot/crawler)
    "TIMESTAMP_OVERFLOW", # malformed epoch timestamp
    "REPEATED_FAILURES",  # same IP hammering 401s
]


# ── Generator ─────────────────────────────────────────────────────────────────

def random_ip():
    return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def generate_row(ts: datetime, inject_anomaly: str | None = None) -> dict:
    """Build one log row, optionally injecting an anomaly."""
    service       = random.choice(SERVICES)
    method        = random.choice(HTTP_METHODS)
    endpoint      = random.choice(ENDPOINTS)
    user_id       = random.randint(1000, 999999)
    ip            = random_ip()
    response_time = random.randint(20, 800)   # ms — normal range
    status        = random.choices(
        STATUS_NORMAL + STATUS_WARN + STATUS_ERROR,
        weights=[70] * len(STATUS_NORMAL) + [20] * len(STATUS_WARN) + [10] * len(STATUS_ERROR),
    )[0]
    level         = "INFO" if status < 400 else ("WARNING" if status < 500 else "ERROR")
    bytes_sent    = random.randint(200, 50000)

    # ── Inject anomaly ──────────────────────────────────────────────────────
    if inject_anomaly == "HIGH_LATENCY":
        response_time = random.randint(5001, 30000)
        level         = "WARNING"

    elif inject_anomaly == "ERROR_SPIKE":
        status = random.choice(STATUS_ERROR)
        level  = "ERROR"

    elif inject_anomaly == "NULL_USER_ID":
        user_id = None      # missing → bot traffic

    elif inject_anomaly == "TIMESTAMP_OVERFLOW":
        ts = datetime(1970, 1, 1)   # epoch zero — malformed

    elif inject_anomaly == "REPEATED_FAILURES":
        status  = 401
        ip      = "192.168.1.1"     # always same attacker IP
        level   = "WARNING"

    return {
        "timestamp":        ts.strftime("%Y-%m-%d %H:%M:%S"),
        "log_level":        level,
        "service":          service,
        "http_method":      method,
        "endpoint":         endpoint,
        "status_code":      status,
        "response_time_ms": response_time,
        "bytes_sent":       bytes_sent,
        "user_id":          user_id,
        "ip_address":       ip,
        "user_agent":       random.choice(USER_AGENTS),
        "anomaly_type":     inject_anomaly or "NONE",   # ground truth label
    }


def generate_logs(n_rows: int, output_path: str, anomaly_rate: float = 0.001) -> None:
    """
    Generate n_rows log entries, injecting anomalies at anomaly_rate frequency.

    Args:
        n_rows:       Total number of rows to generate.
        output_path:  Destination CSV file path.
        anomaly_rate: Fraction of rows that are anomalies (default 0.1%).
    """
    start_ts  = datetime(2024, 1, 15, 0, 0, 0)
    fieldnames = [
        "timestamp", "log_level", "service", "http_method",
        "endpoint", "status_code", "response_time_ms", "bytes_sent",
        "user_id", "ip_address", "user_agent", "anomaly_type",
    ]

    print(f"[generate_logs] Generating {n_rows:,} rows → {output_path}")
    anomaly_count = 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(n_rows):
            # Advance timestamp with small random jitter (realistic log stream)
            start_ts += timedelta(milliseconds=random.randint(1, 50))

            # Decide anomaly injection
            inject = None
            if random.random() < anomaly_rate:
                inject = random.choice(ANOMALY_SCENARIOS)
                anomaly_count += 1

            writer.writerow(generate_row(start_ts, inject))

            if (i + 1) % 100_000 == 0:
                pct = (i + 1) / n_rows * 100
                print(f"  {i+1:>9,} rows written ({pct:.0f}%)")

    print(f"[generate_logs] Done — {anomaly_count:,} anomalies injected ({anomaly_count/n_rows*100:.2f}%)")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic log data generator")
    parser.add_argument("--rows",   type=int,   default=1_000_000, help="Number of log rows")
    parser.add_argument("--output", type=str,   default="data/logs.csv", help="Output CSV path")
    parser.add_argument("--anomaly-rate", type=float, default=0.001, help="Fraction of anomalous rows")
    args = parser.parse_args()

    generate_logs(args.rows, args.output, args.anomaly_rate)
