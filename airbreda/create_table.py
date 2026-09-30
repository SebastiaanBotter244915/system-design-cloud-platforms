"""
create_table.py — creates the sensor_readings table for AirBreda (Day 1, Lab 2).

Safe to re-run: uses CREATE TABLE IF NOT EXISTS (the lab's schema is unchanged,
this just adds idempotency so re-running this script doesn't error out).

Day 2, Lab 2 migrations (also idempotent, so this upgrades an existing table):
  - is_flagged BOOLEAN DEFAULT FALSE, set TRUE for stale/null air readings.
  - station_id widened to VARCHAR(40): NDW site IDs such as
    RWS01_MONIBAS_0271hrl0063ra are 27 characters.

After creating, it prints the table's columns and row count as a sanity check.
"""

import os
import sys

import psycopg2

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sensor_readings (
    station_id  VARCHAR(40)   NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,
    component   VARCHAR(10)   NOT NULL,
    value       FLOAT,
    is_flagged  BOOLEAN       DEFAULT FALSE,
    PRIMARY KEY (station_id, timestamp, component)
);
"""

MIGRATE_SQL = """
ALTER TABLE sensor_readings ADD COLUMN IF NOT EXISTS is_flagged BOOLEAN DEFAULT FALSE;
ALTER TABLE sensor_readings ALTER COLUMN station_id TYPE VARCHAR(40);
"""

CHECK_SQL = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'sensor_readings'
ORDER BY ordinal_position;
"""


def main() -> int:
    try:
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", "5432"),
            dbname=os.environ.get("DB_NAME", "postgres"),
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            connect_timeout=10,
        )
    except KeyError as exc:
        print(f"Missing environment variable: {exc}")
        return 1
    except psycopg2.OperationalError as exc:
        print(f"Could not connect: {exc}")
        return 1

    with conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute(MIGRATE_SQL)
            cur.execute(CHECK_SQL)
            columns = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM sensor_readings;")
            (row_count,) = cur.fetchone()

    print("sensor_readings table created (or already existed).")
    print("Columns:")
    for name, dtype in columns:
        print(f"  {name}: {dtype}")
    print(f"Rows: {row_count}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())