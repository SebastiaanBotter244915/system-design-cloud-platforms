"""
dashboard.py — AirBreda dashboard + API (Day 5, Lab 2).

  GET /site/{site_id}  latest NO2 (sensor_readings, station NL10240), that
                       site's latest intensity (S3 bucket), and predict()
  GET /interchange     the four sites' intensities summed, and predict() on
                       that total (the input the model was trained on)
  GET /history         NO2 at NL10240 over the last N hours, for the chart
  GET /health          freshness of the latest reading per source
                       (Luchtmeetnet NO2, NDW traffic), read from
                       sensor_readings
  GET /                dashboard.html, just another client of the endpoints above

Run locally: uvicorn dashboard:app --port 8000
"""

import io
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from predict import NO2_THRESHOLD, predict

STATION = "NL10240"
SITE_LABELS = ["hrl", "hrr", "vwd", "vwa"]
BUCKET_NAME = os.environ.get("S3_BUCKET", "airbreda-data-bucket")
S3_PREFIX = "ndw/"

# A source counts as "up" if its most recent row is newer than this many
# minutes ago. Both sources ingest hourly, so this is 60 + a buffer for
# normal run-time drift and cron delay, not a tight SLA.
STALE_THRESHOLD_MINUTES = 75

LATEST_NO2_SQL = """
SELECT value, timestamp
FROM sensor_readings
WHERE station_id = %s
  AND component = 'NO2'
  AND value IS NOT NULL
  AND NOT is_flagged
ORDER BY timestamp DESC
LIMIT 1;
"""

NO2_HISTORY_SQL = """
SELECT value, timestamp
FROM sensor_readings
WHERE station_id = %s
  AND component = 'NO2'
  AND value IS NOT NULL
  AND NOT is_flagged
  AND timestamp >= now() - make_interval(hours => %s)
ORDER BY timestamp;
"""

# Groups every row by source: anything stored under the air station's ID is
# Luchtmeetnet, everything else is one of the four NDW site IDs.
HEALTH_SQL = """
SELECT
    CASE WHEN station_id = %s THEN 'Luchtmeetnet' ELSE 'NDW' END AS source,
    MAX(timestamp) AS last_seen
FROM sensor_readings
GROUP BY 1;
"""

PAGE = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")

app = FastAPI(title="AirBreda")


def query(sql: str, params: tuple) -> list[tuple]:
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    conn.close()
    return rows


def latest_no2() -> tuple[float, pd.Timestamp]:
    rows = query(LATEST_NO2_SQL, (STATION,))
    if not rows:
        raise HTTPException(503, "no NO2 reading in sensor_readings")
    return float(rows[0][0]), pd.Timestamp(rows[0][1])


def newest_keys(s3) -> dict[str, str]:
    """Newest ndw/YYYY-MM-DD/HH-{site}.csv per site, from one bucket listing.
    The keys sort by date and hour, so the newest file is simply the largest key."""
    newest = {}
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET_NAME, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            for site_id in SITE_LABELS:
                if obj["Key"].endswith(f"-{site_id}.csv"):
                    newest[site_id] = max(newest.get(site_id, ""), obj["Key"])
    return newest


def read_intensity(s3, key: str) -> tuple[float, pd.Timestamp]:
    body = s3.get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read()
    row = pd.read_csv(io.BytesIO(body)).iloc[0]
    return float(row["total_flow"]), pd.to_datetime(row["timestamp"], utc=True)


def latest_intensity(site_id: str) -> tuple[float, pd.Timestamp]:
    s3 = boto3.client("s3")
    key = newest_keys(s3).get(site_id)
    if key is None:
        raise HTTPException(503, f"no traffic file for {site_id} in the bucket")
    return read_intensity(s3, key)


def safe_predict(intensity: float, ts: pd.Timestamp) -> tuple[float | None, float | None]:
    """predict() rounded, or (None, None) if it raises, so the request still
    returns the real values."""
    try:
        # Same hour_of_day as the training data: UTC, rounded to the nearest hour.
        prediction = predict(intensity, ts.round("h").hour)
        return (round(prediction["no2_ug_m3_predicted"], 2),
                round(prediction["no2_exceedance_risk"], 3))
    except Exception:
        return None, None


def pipeline_health() -> list[dict]:
    """Freshness per source, read directly from sensor_readings instead of
    pinging the ingest containers over HTTP: on this VM, air-ingest and
    traffic-ingest run as one-shot cron jobs (docker run --rm ... --once),
    so they aren't listening on any port between runs - an HTTP health
    check would see "connection refused" almost all the time even when
    ingestion is working perfectly. The database is the one thing both
    ingestion paths always write to, so it's the honest source of truth
    for "is data still arriving"."""
    rows = query(HEALTH_SQL, (STATION,))
    now = datetime.now(timezone.utc)
    sources = []
    for source, last_seen in rows:
        age_minutes = (now - last_seen).total_seconds() / 60
        sources.append({
            "source": source,
            "last_successful_fetch": last_seen.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": "up" if age_minutes <= STALE_THRESHOLD_MINUTES else "down",
            "age_minutes": round(age_minutes, 1),
        })
    return sources


@app.get("/site/{site_id}")
def site(site_id: str):
    """Where each response field comes from:
      - site_id:              the URL path
      - no2_ug_m3:            latest_no2(), newest unflagged NO2 row for
                              NL10240 in sensor_readings (Postgres)
      - intensity_veh_per_hr: latest_intensity(), total_flow from the newest
                              ndw/.../HH-{site}.csv in the S3 bucket
      - no2_ug_m3_predicted,
        no2_exceedance_risk:  predict() from predict.py (model.pkl)
      - timestamp:            the timestamp of that S3 traffic reading

    If predict() raises, the request doesn't fail: the real NO2 and intensity
    values are still returned, with the two prediction fields set to null.
    """
    if site_id not in SITE_LABELS:
        raise HTTPException(404, f"unknown site, use one of {SITE_LABELS}")
    no2, _ = latest_no2()
    intensity, ts = latest_intensity(site_id)
    predicted, risk = safe_predict(intensity, ts)
    return {
        "site_id": site_id,
        "no2_ug_m3": no2,
        "no2_ug_m3_predicted": predicted,
        "intensity_veh_per_hr": intensity,
        "no2_exceedance_risk": risk,
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@app.get("/interchange")
def interchange():
    """The four sites summed. The model was trained on this total, so this is
    the prediction to trust; the per-site ones in /site/{id} extrapolate
    (see ADR-006). timestamp is the oldest of the four readings."""
    s3 = boto3.client("s3")
    keys = newest_keys(s3)
    missing = [s for s in SITE_LABELS if s not in keys]
    if missing:
        raise HTTPException(503, f"no traffic file for {missing} in the bucket")
    readings = [read_intensity(s3, keys[s]) for s in SITE_LABELS]
    total = sum(intensity for intensity, _ in readings)
    ts = min(t for _, t in readings)
    predicted, risk = safe_predict(total, ts)
    return {
        "total_intensity_veh_per_hr": total,
        "no2_ug_m3_predicted": predicted,
        "no2_exceedance_risk": risk,
        "threshold_ug_m3": NO2_THRESHOLD,
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@app.get("/history")
def history(hours: int = Query(48, ge=1, le=168)):
    """Unflagged hourly NO2 at NL10240, oldest first."""
    rows = query(NO2_HISTORY_SQL, (STATION, hours))
    return {
        "station_id": STATION,
        "threshold_ug_m3": NO2_THRESHOLD,
        "readings": [
            {"timestamp": pd.Timestamp(ts).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
             "no2_ug_m3": float(value)}
            for value, ts in rows
        ],
    }


@app.get("/health")
def health():
    return {"sources": pipeline_health()}


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE