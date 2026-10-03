"""
build_training_data.py — AirBreda

Builds training_data.csv: one row per hour with the NO2 reading for station
NL10240 and the traffic intensity at the four A27 sites.

  - NO2 comes from sensor_readings (component 'NO2'). Null and flagged
    (stale) readings are left out: they can't be used as a training target.
  - Traffic comes from the NDW CSVs in S3 (ndw/YYYY-MM-DD/HH-{site}.csv).
    The site label is taken from the file name, the intensity is the CSV's
    total_flow (veh/h).
  - Both timestamps are rounded to the nearest hour and inner-joined. An hour
    is only kept if all four sites have a value, so total_intensity_veh_per_hr
    always sums the same four sites.

Note: Luchtmeetnet stamps hourly averages at the *end* of the hour, while
the NDW value is a one-minute snapshot. So NO2 at 18:00 is the 17:00-18:00
average, joined with the traffic snapshot taken around 18:00.
"""

import io
import os
import sys

import boto3
import pandas as pd
import psycopg2

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

STATION = "NL10240"
SITE_LABELS = ["hrl", "hrr", "vwd", "vwa"]
BUCKET_NAME = os.environ.get("S3_BUCKET", "airbreda-data-bucket")
S3_PREFIX = "ndw/"
OUTPUT_PATH = "training_data.csv"

NO2_SQL = """
SELECT timestamp, value AS no2_ug_m3
FROM sensor_readings
WHERE station_id = %s
  AND component = 'NO2'
  AND value IS NOT NULL
  AND NOT is_flagged
ORDER BY timestamp;
"""


def load_no2() -> pd.DataFrame:
    """All usable NO2 readings for STATION, one row per hour."""
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
            cur.execute(NO2_SQL, (STATION,))
            rows = cur.fetchall()
    conn.close()

    df = pd.DataFrame(rows, columns=["timestamp", "no2_ug_m3"])
    df["hour"] = pd.to_datetime(df["timestamp"], utc=True).dt.round("h")
    return df.groupby("hour", as_index=False)["no2_ug_m3"].mean()


def site_label_from_key(key: str) -> str | None:
    """'ndw/2026-09-30/18-hrl.csv' -> 'hrl'. None for anything else."""
    name = key.rsplit("/", 1)[-1]
    if not name.endswith(".csv") or "-" not in name:
        return None
    label = name[:-len(".csv")].split("-", 1)[1]
    return label if label in SITE_LABELS else None


def load_traffic() -> pd.DataFrame:
    """Every NDW CSV in the bucket, pivoted to one intensity column per site."""
    s3 = boto3.client("s3")
    records = []
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET_NAME, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            label = site_label_from_key(obj["Key"])
            if label is None:
                continue
            body = s3.get_object(Bucket=BUCKET_NAME, Key=obj["Key"])["Body"].read()
            csv = pd.read_csv(io.BytesIO(body))
            for _, row in csv.iterrows():
                records.append({
                    "site": label,
                    "timestamp": row["timestamp"],
                    "intensity": row["total_flow"],
                })

    df = pd.DataFrame(records, columns=["site", "timestamp", "intensity"])
    df["hour"] = pd.to_datetime(df["timestamp"], utc=True).dt.round("h")
    wide = df.pivot_table(index="hour", columns="site", values="intensity",
                          aggfunc="mean")
    wide = wide.reindex(columns=SITE_LABELS)
    wide.columns = [f"intensity_{label}_veh_per_hr" for label in SITE_LABELS]
    return wide.reset_index()


def build(no2: pd.DataFrame, traffic: pd.DataFrame) -> pd.DataFrame:
    df = no2.merge(traffic, on="hour", how="inner")
    site_cols = [f"intensity_{label}_veh_per_hr" for label in SITE_LABELS]
    df = df.dropna(subset=site_cols)
    df["total_intensity_veh_per_hr"] = df[site_cols].sum(axis=1)
    df["hour_of_day"] = df["hour"].dt.hour
    return df.rename(columns={"hour": "timestamp"}).sort_values("timestamp")


def main() -> int:
    try:
        no2 = load_no2()
    except (KeyError, psycopg2.Error) as exc:
        print(f"Could not read sensor_readings: {exc}")
        return 1
    traffic = load_traffic()

    df = build(no2, traffic)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"NO2 hours: {len(no2)}, traffic hours: {len(traffic)}, "
          f"joined rows: {len(df)} -> {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
