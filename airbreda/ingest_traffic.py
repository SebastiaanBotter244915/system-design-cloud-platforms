"""
ingest_traffic.py — AirBreda 

Day 1: Downloads the NDW site-config and live measurement feeds, extracts the four
A27/Breda sites (hrl, hrr, vwd, vwa), saves each site's reading as a
timestamped CSV under ndw/YYYY-MM-DD/HH-{site}.csv, and uploads it to S3.
---

Day 2, Lab 1: also publishes one JSON message per site to the Redis list `readings`,
and polls on an interval instead of running once (see POLL_INTERVAL_SECONDS).
Day 2, Lab 2: logs structured JSON, writes each site's flow/speed to
sensor_readings, and skips that write for any site reporting NDW's speed=-1
sentinel (logged as DATA_QUALITY_ERROR and counted in ndw_monitor). Serves
GET /health on HEALTH_PORT.

Reuses the XML parsing helpers from getTrafficReadings.py
(download_and_decompress, build_index_map, extract_measurements, report_site)
rather than reimplementing DATEX II parsing from scratch.
"""

import csv
import json
import os
import sys
import time
from datetime import datetime
import logging

import boto3
import psycopg2
import redis

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from getTrafficReadings import (
    CONFIG_URL,
    MEASURED_URL,
    TARGET_SITE_IDS,
    download_and_decompress,
    report_site,
)
from observability import SourceMonitor, setup_logging, start_health_server

SOURCE = "NDW"
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8001"))

# In-memory bad_data_count for this source, served on /health.
ndw_monitor = SourceMonitor(SOURCE)

# NDW reports -1 for a lane with no valid speed measurement this minute.
SPEED_SENTINEL = -1

INSERT_SQL = """
INSERT INTO sensor_readings (station_id, timestamp, component, value)
VALUES (%s, %s::timestamptz, %s, %s)
ON CONFLICT (station_id, timestamp, component) DO NOTHING;
"""

# Same order as TARGET_SITE_IDS in getTrafficReadings.py: mainline dir 1,
# mainline dir 2, entry slip road, exit slip road.
SITE_LABELS = ["hrl", "hrr", "vwd", "vwa"]

BUCKET_NAME = os.environ.get("S3_BUCKET", "airbreda-data-bucket")

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")  # compose service name
REDIS_LIST = "readings"
"""
Polling interval: once per hour, matching ingest_air.py. NDW updates every
minute, but the air-quality side is hourly, and the whole point of AirBreda is
joining the two on time: a one-minute traffic snapshot per hour is what
lines up with one NO2 reading per hour. If we polled every minute instead:
  - each run downloads two full-country gzipped DATEX II files (tens of MB,
    with a 180 s timeout), so runs could take longer than the interval and
    pile up, while hammering NDW's open-data servers ~60x more;
  - the S3 key ndw/YYYY-MM-DD/HH-{site}.csv is per *hour*, so 60 uploads an
    hour would silently overwrite each other, keeping only the last one;
  - 240 messages/hour would flood `readings`, drowning the 1 NO2 message/hour
    consumers need to pair them with.
Overridable via env for testing (e.g. POLL_INTERVAL_SECONDS=120).
"""
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))

"""
WHY BOTH THE DATABASE AND THE BUCKET:
---

The relational database (sensor_readings) is optimised for structured,
indexed queries — time-range lookups, joins between traffic and NO2 by
timestamp — but it only stores the numbers we chose to compute *today*
(total_flow, avg_speed). Object storage (S3) keeps a durable copy of the
raw, timestamped CSV snapshots as they were actually parsed, independent
of any one processing decision. If we need to retrain the ML model six
months from now with a different feature set (e.g. per-lane speed instead
of the average, or a different aggregation window), the database alone
can't give that back — those columns don't exist there. 

The S3 archive can,because it's the raw material, not a derived summary. 
The bucket is our ability to change our minds later; the database is our 
ability to query fast today.
"""

def parse_measurement_dt(ts: str) -> datetime:
    """Parse the measurement's own timestamp (not today's date) for use in
    the ndw/YYYY-MM-DD/HH-{site}.csv path."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def save_and_upload(summary: dict, label: str) -> str:
    dt = parse_measurement_dt(summary["timestamp"])
    date_str = dt.strftime("%Y-%m-%d")
    hour_str = dt.strftime("%H")

    local_dir = os.path.join("ndw", date_str)
    os.makedirs(local_dir, exist_ok=True)
    filename = f"{hour_str}-{label}.csv"
    local_path = os.path.join(local_dir, filename)

    with open(local_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["site_id", "total_flow", "avg_speed", "timestamp"],
            extrasaction="ignore",  # lane_speeds isn't part of the CSV
        )
        writer.writeheader()
        writer.writerow(summary)

    s3_key = f"ndw/{date_str}/{filename}"
    boto3.client("s3").upload_file(local_path, BUCKET_NAME, s3_key)
    return s3_key


def to_message(summary: dict) -> dict:
    """Shape one site summary as the `readings` queue message. Same keys as
    the air messages; the site ID plays the role of station_id and total_flow
    (vehicles/hour) is the value, with avg_speed carried alongside."""
    return {
        "station_id": summary["site_id"],
        "timestamp": summary["timestamp"],
        "component": "traffic_flow",
        "value": summary["total_flow"],
        "avg_speed": summary["avg_speed"],
    }


def write_site_reading(summary: dict) -> int:
    """Write one site's flow and speed into sensor_readings as two rows
    (component 'flow' in veh/h, 'speed' in km/h). Idempotent via ON CONFLICT."""
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
            for component, value in (("flow", summary["total_flow"]),
                                     ("speed", summary["avg_speed"])):
                cur.execute(INSERT_SQL, (summary["site_id"], summary["timestamp"],
                                         component, value))
                inserted += cur.rowcount
    conn.close()
    return inserted


def handle_site(summary: dict, label: str, r: redis.Redis) -> None:
    """Quality-check one site's reading, then send it to S3, the database
    and Redis. A speed=-1 reading still goes to S3 (the raw archive) and
    Redis (its avg_speed already excludes the -1 lanes), but is never
    written to sensor_readings."""
    site_id = summary["site_id"]
    has_sentinel = SPEED_SENTINEL in summary.get("lane_speeds", [])

    if has_sentinel:
        logging.warning(json.dumps({
            "event": "DATA_QUALITY_ERROR", "source": SOURCE,
            "location": site_id, "field": "speed", "value": SPEED_SENTINEL,
            "timestamp": summary["timestamp"],
        }))
        ndw_monitor.record_bad_data()
    else:
        logging.info(json.dumps({
            "event": "fetch_success", "source": SOURCE,
            "location": site_id, "value": summary["total_flow"],
            "avg_speed": summary["avg_speed"], "timestamp": summary["timestamp"],
        }))

    # S3, DB and Redis are independent sinks: one failing shouldn't block the others.
    try:
        key = save_and_upload(summary, label)
        logging.info(json.dumps({
            "event": "s3_upload", "source": SOURCE, "location": site_id,
            "key": f"s3://{BUCKET_NAME}/{key}",
        }))
    except Exception as exc:  # botocore raises several unrelated types
        logging.error(json.dumps({
            "event": "s3_upload_failed", "source": SOURCE, "location": site_id,
            "error": str(exc),
        }))

    if has_sentinel:
        logging.info(json.dumps({
            "event": "db_write_skipped", "source": SOURCE, "location": site_id,
            "reason": "speed_sentinel",
        }))
    else:
        try:
            written = write_site_reading(summary)
            logging.info(json.dumps({
                "event": "db_write", "source": SOURCE, "table": "sensor_readings",
                "location": site_id, "rows_written": written,
            }))
        except (KeyError, psycopg2.Error) as exc:
            logging.error(json.dumps({
                "event": "db_write_failed", "source": SOURCE, "location": site_id,
                "error": str(exc),
            }))

    try:
        r.rpush(REDIS_LIST, json.dumps(to_message(summary)))
        logging.info(json.dumps({
            "event": "redis_publish", "source": SOURCE, "location": site_id,
            "list": REDIS_LIST, "messages": 1,
        }))
    except redis.exceptions.RedisError as exc:
        logging.error(json.dumps({
            "event": "redis_publish_failed", "source": SOURCE, "location": site_id,
            "error": str(exc),
        }))


def main() -> int:
    try:
        config_bytes = download_and_decompress(CONFIG_URL).read()
        measured_bytes = download_and_decompress(MEASURED_URL).read()
    except OSError as exc:  # urllib's URLError/timeouts are OSError subclasses
        logging.error(json.dumps({
            "event": "fetch_failed", "source": SOURCE, "error": str(exc),
        }))
        return 0
    ndw_monitor.record_success()

    r = redis.Redis(host=REDIS_HOST, port=6379)

    for site_id, label in zip(TARGET_SITE_IDS, SITE_LABELS):
        summary = report_site(config_bytes, measured_bytes, site_id, verbose=False)
        if summary.get("timestamp") is None:
            logging.warning(json.dumps({
                "event": "fetch_empty", "source": SOURCE, "location": site_id,
            }))
            continue
        handle_site(summary, label, r)

    return 0


if __name__ == "__main__":
    setup_logging()
    if "--once" in sys.argv:
        sys.exit(main())
    start_health_server(ndw_monitor, HEALTH_PORT)
    while True:
        try:
            main()
        except Exception as exc:  # keep polling; one bad run shouldn't kill the container
            logging.error(json.dumps({
                "event": "run_failed", "source": SOURCE, "error": repr(exc),
            }))
        time.sleep(POLL_INTERVAL_SECONDS)
