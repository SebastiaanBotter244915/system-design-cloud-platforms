import pandas as pd
import pytest

import ingest_air
import ingest_traffic


class FakeCursor:
    def __init__(self, executed):
        self.executed = executed
        self.rowcount = 1

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    """Stands in for psycopg2's connection: records every INSERT so the tests
    can check exactly what would have reached sensor_readings."""

    def __init__(self):
        self.executed = []

    def cursor(self):
        return FakeCursor(self.executed)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def fake_db(monkeypatch):
    conn = FakeConnection()
    monkeypatch.setattr("psycopg2.connect", lambda **kwargs: conn)
    for var in ("DB_HOST", "DB_USER", "DB_PASSWORD"):
        monkeypatch.setenv(var, "test")
    return conn


def test_stale_or_null_luchtmeetnet_reading_is_written_flagged_not_dropped(fake_db, monkeypatch):
    monkeypatch.setattr(ingest_air, "luchtmeetnet_monitor", ingest_air.SourceMonitor("Luchtmeetnet"))
    monkeypatch.setattr(ingest_air, "_reported_bad", set())
    no2 = pd.DataFrame({
        "component": ["NO2"] * 5,
        "value": [20.1, None, 18.4, 18.4, 18.4],
        "timestamp": [
            "2024-01-15T05:00:00+00:00",  # clean
            "2024-01-15T06:00:00+00:00",  # null
            "2024-01-15T07:00:00+00:00",  # 18.4 x3 consecutive hours: stale
            "2024-01-15T08:00:00+00:00",
            "2024-01-15T09:00:00+00:00",
        ],
    })

    checked = ingest_air.check_data_quality(no2)
    ingest_air.write_readings(checked)

    # Every reading reaches sensor_readings: nothing is dropped.
    assert len(fake_db.executed) == 5
    # params: (station_id, timestamp, component, value, is_flagged)
    written = {params[1]: params for _, params in fake_db.executed}
    assert written["2024-01-15T06:00:00+00:00"][3] is None  # null stored as SQL NULL
    assert written["2024-01-15T06:00:00+00:00"][4] is True
    for hour in ("07", "08", "09"):
        assert written[f"2024-01-15T{hour}:00:00+00:00"][4] is True
    assert written["2024-01-15T05:00:00+00:00"][4] is False
    assert ingest_air.luchtmeetnet_monitor.bad_data_count == 4


class FakeRedis:
    def __init__(self):
        self.pushed = []

    def rpush(self, key, *values):
        self.pushed.extend(values)


def test_ndw_speed_sentinel_row_is_not_written_and_counted(fake_db, monkeypatch):
    monkeypatch.setattr(ingest_traffic, "ndw_monitor", ingest_traffic.SourceMonitor("NDW"))
    monkeypatch.setattr(ingest_traffic, "save_and_upload", lambda summary, label: "ndw/test.csv")
    summary = {
        "site_id": "RWS01_MONIBAS_0271hrl0063ra",
        "total_flow": 2100.0,
        "avg_speed": 102.5,
        "timestamp": "2024-01-15T09:00:00Z",
        "lane_speeds": [102.5, -1.0],  # one lane reports the -1 sentinel
    }

    ingest_traffic.handle_site(summary, "hrl", FakeRedis())

    assert fake_db.executed == []  # nothing reached sensor_readings
    assert ingest_traffic.ndw_monitor.bad_data_count == 1


def test_ndw_clean_row_is_written(fake_db, monkeypatch):
    """Control for the test above: without the sentinel, the row IS written."""
    monkeypatch.setattr(ingest_traffic, "ndw_monitor", ingest_traffic.SourceMonitor("NDW"))
    monkeypatch.setattr(ingest_traffic, "save_and_upload", lambda summary, label: "ndw/test.csv")
    summary = {
        "site_id": "RWS01_MONIBAS_0271hrl0063ra",
        "total_flow": 2100.0,
        "avg_speed": 102.5,
        "timestamp": "2024-01-15T09:00:00Z",
        "lane_speeds": [102.5, 101.0],
    }

    ingest_traffic.handle_site(summary, "hrl", FakeRedis())

    assert [params[2] for _, params in fake_db.executed] == ["flow", "speed"]
    assert ingest_traffic.ndw_monitor.bad_data_count == 0


def test_bad_data_threshold_logs_single_error(caplog):
    monitor = ingest_traffic.SourceMonitor("NDW", clock=lambda: 0.0)
    for _ in range(15):
        monitor.record_bad_data()

    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert '"BAD_DATA_THRESHOLD_EXCEEDED"' in errors[0].getMessage()
    assert '"count": 11' in errors[0].getMessage()
