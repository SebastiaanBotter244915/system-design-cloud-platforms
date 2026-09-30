"""
ingest_air.py — AirBreda 

Day 1
Fetches the station's latest readings across all components, filters to NO2,
and writes each reading into the sensor_readings table. Idempotent: running
this twice does not create duplicate rows (INSERT ... ON CONFLICT DO NOTHING
on the station_id/timestamp/component primary key).
---

Day 2, Lab 1: each NO2 reading is also published as a JSON message to the Redis
list `readings`, and the script now polls on an interval instead of running
once (see POLL_INTERVAL_SECONDS).
Day 2, Lab 2: logs structured JSON, flags stale/null readings (is_flagged =
TRUE, still written; counted in luchtmeetnet_monitor), and serves GET /health
on HEALTH_PORT.

Builds on the same station/endpoint used in getNO2Readings.py's
get_latest_no2(), but fetches without a `formula` filter so the NO2 filtering
happens here in code rather than server-side.
"""

import json
import os
import sys
import time
import logging

import pandas as pd
import psycopg2
import redis
import requests

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from getNO2Readings import STATION, BASE_URL
from observability import SourceMonitor, setup_logging, start_health_server

SOURCE = "Luchtmeetnet"
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8000"))

# In-memory bad_data_count for this source, served on /health.
luchtmeetnet_monitor = SourceMonitor(SOURCE)

# A value repeated for this many consecutive hours is treated as stale
# (a stuck sensor, or the API interpolating over a gap).
STALE_RUN_LENGTH = 3

# (timestamp) of readings already reported as bad. Each hourly poll re-fetches
# the last ~10 hours, so without this one null reading would be logged and
# counted again on every run until it scrolls out of the page.
_reported_bad = set()

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")  # compose service name
REDIS_LIST = "readings"
"""
Polling interval: once per hour. Luchtmeetnet only publishes hourly averages
(stamped at the end of the hour), so polling more often can't return new
data. If we polled every minute instead:
  - the same hourly reading would be pushed onto `readings` ~60 times. The
    database is protected by ON CONFLICT DO NOTHING, but a Redis list has no
    such key, so every downstream consumer would receive 60 duplicates per
    hour and have to deduplicate them itself;
  - we'd make 60x the requests against a free public API with a fair-use
    limit (100 req / 5 min) for zero extra information;
  - the queue would grow ~60x faster, filling Redis memory with repeats.
Overridable via env for testing (e.g. POLL_INTERVAL_SECONDS=30).
"""
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))

# A reading can only be recognised as stale once the 3rd identical hour
# arrives, by which time the first two are already stored unflagged. So on
# conflict we still insert nothing new, but do upgrade is_flagged to TRUE.
# It never goes back to FALSE.
INSERT_SQL = """
INSERT INTO sensor_readings (station_id, timestamp, component, value, is_flagged)
VALUES (%s, %s::timestamptz, %s, %s, %s)
ON CONFLICT (station_id, timestamp, component) DO UPDATE
    SET is_flagged = TRUE
    WHERE EXCLUDED.is_flagged AND NOT sensor_readings.is_flagged;
"""


def fetch_latest_readings(station: str = STATION) -> pd.DataFrame:
    """Fetch the station's latest readings across ALL components (no formula
    filter) and return them as a DataFrame with component/value/timestamp columns."""
    params = {
        "order_by": "timestamp_measured",
        "order_direction": "desc",
        "page": 1,
    }
    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    measurements = response.json().get("data", [])

    return pd.DataFrame([
        {
            "component": m["formula"],
            "value": m["value"],
            "timestamp": m["timestamp_measured"],
        }
        for m in measurements
    ])

"""
CAP trade-off: Luchtmeetnet's API favours availability over consistency
(an AP-style choice). If a sensor station loses connectivity, the API
can still return a response for that hour, but the value may come back
null or stale instead of a true up-to-date reading. We keep null NO2
rows here rather than silently dropping them, because a null is itself
a meaningful data-quality signal a production pipeline must detect and
flag (e.g. for alerting or later imputation) — not something to hide.
"""
def filter_no2_readings(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["component"] == "NO2"]


def flag_stale_or_null(df: pd.DataFrame) -> pd.Series:
    """Boolean Series (aligned to df.index): True where the value is null, or
    is part of a run of STALE_RUN_LENGTH+ identical values at consecutive
    hourly timestamps."""
    ordered = df.assign(_ts=pd.to_datetime(df["timestamp"], utc=True)).sort_values("_ts")
    new_run = (
        (ordered["value"] != ordered["value"].shift())
        | (ordered["_ts"].diff() != pd.Timedelta(hours=1))
    )
    run_length = ordered.groupby(new_run.cumsum())["value"].transform("size")
    stale = (run_length >= STALE_RUN_LENGTH) & ordered["value"].notna()
    return (stale | ordered["value"].isna()).reindex(df.index)


def check_data_quality(df: pd.DataFrame, station: str = STATION) -> pd.DataFrame:
    """Return df with an is_flagged column. Flagged rows are kept (a gap in
    the air-quality series is worse than a flagged value) but each one gets a
    DATA_QUALITY_ERROR warning and increments luchtmeetnet_monitor's
    bad_data_count. Clean rows get a fetch_success event."""
    df = df.assign(is_flagged=flag_stale_or_null(df))
    for _, row in df.iterrows():
        value = None if pd.isna(row["value"]) else float(row["value"])
        timestamp = str(row["timestamp"]).replace("+00:00", "Z")
        if not row["is_flagged"]:
            logging.info(json.dumps({
                "event": "fetch_success", "source": SOURCE,
                "station_id": station, "value": value,
                "timestamp": timestamp,
            }))
        elif timestamp not in _reported_bad:
            _reported_bad.add(timestamp)
            logging.warning(json.dumps({
                "event": "DATA_QUALITY_ERROR", "source": SOURCE,
                "station_id": station, "field": row["component"],
                "reason": "stale_or_null", "value": value,
                "timestamp": timestamp,
            }))
            luchtmeetnet_monitor.record_bad_data()
    return df


def write_readings(df: pd.DataFrame, station: str = STATION) -> int:
    """Write each row into sensor_readings, including flagged ones (with
    is_flagged = TRUE). Idempotent: re-inserting an already-seen
    (station_id, timestamp, component) row never duplicates it; it can only
    upgrade is_flagged (see INSERT_SQL). Returns rows inserted or newly flagged."""
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )
    inserted = 0
    with conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                value = None if pd.isna(row["value"]) else float(row["value"])
                is_flagged = bool(row.get("is_flagged", False))
                cur.execute(
                    INSERT_SQL,
                    (station, row["timestamp"], row["component"], value, is_flagged),
                )
                inserted += cur.rowcount  # 0 if ON CONFLICT changed nothing
    conn.close()
    return inserted


def to_message(row: pd.Series, station: str = STATION) -> dict:
    """Shape one NO2 row as the `readings` queue message."""
    return {
        "station_id": station,
        # Luchtmeetnet returns "+00:00"; normalise to the "Z" form.
        "timestamp": str(row["timestamp"]).replace("+00:00", "Z"),
        "component": row["component"],
        # Null stays null in JSON, same data-quality signal as in the DB.
        "value": None if pd.isna(row["value"]) else float(row["value"]),
        # Extra field so consumers can tell a stale/null reading from a real one.
        "is_flagged": bool(row.get("is_flagged", False)),
    }


def publish_readings(df: pd.DataFrame, station: str = STATION) -> int:
    """RPUSH each row as a JSON message onto the Redis `readings` list."""
    r = redis.Redis(host=REDIS_HOST, port=6379)
    messages = [json.dumps(to_message(row, station)) for _, row in df.iterrows()]
    if messages:
        r.rpush(REDIS_LIST, *messages)
    return len(messages)


def main() -> int:
    try:
        readings = fetch_latest_readings()
    except requests.exceptions.RequestException as exc:
        logging.error(json.dumps({
            "event": "fetch_failed", "source": SOURCE, "error": str(exc),
        }))
        return 0

    no2 = filter_no2_readings(readings)
    if no2.empty:
        logging.warning(json.dumps({
            "event": "fetch_empty", "source": SOURCE, "station_id": STATION,
        }))
        return 0

    luchtmeetnet_monitor.record_success()
    no2 = check_data_quality(no2)

    # DB and Redis are independent sinks: one failing shouldn't block the other.
    try:
        written = write_readings(no2)
        logging.info(json.dumps({
            "event": "db_write", "source": SOURCE, "table": "sensor_readings",
            "rows_written": written, "rows_unchanged": len(no2) - written,
            "rows_flagged": int(no2["is_flagged"].sum()),
        }))
    except (KeyError, psycopg2.Error) as exc:
        logging.error(json.dumps({
            "event": "db_write_failed", "source": SOURCE, "error": str(exc),
        }))

    try:
        published = publish_readings(no2)
        logging.info(json.dumps({
            "event": "redis_publish", "source": SOURCE, "list": REDIS_LIST,
            "messages": published,
        }))
    except redis.exceptions.RedisError as exc:
        logging.error(json.dumps({
            "event": "redis_publish_failed", "source": SOURCE, "error": str(exc),
        }))

    return 0


if __name__ == "__main__":
    setup_logging()
    if "--once" in sys.argv:
        sys.exit(main())
    start_health_server(luchtmeetnet_monitor, HEALTH_PORT)
    while True:
        try:
            main()
        except Exception as exc:  # keep polling; one bad run shouldn't kill the container
            logging.error(json.dumps({
                "event": "run_failed", "source": SOURCE, "error": repr(exc),
            }))
        time.sleep(POLL_INTERVAL_SECONDS)