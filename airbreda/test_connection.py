"""
test_connection.py — verify we can reach the RDS PostgreSQL instance
before wiring up the full ingest_air.py write path.

Reads connection details from environment variables so credentials never
end up hardcoded in a script or committed to git.
"""

import os
import sys

import psycopg2

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed locally — fine if vars are already set


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
            cur.execute("SELECT version();")
            print("Connected! Server says:", cur.fetchone()[0])

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())