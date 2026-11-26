from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import threading

from fastapi.testclient import TestClient

from app.health import HealthConfig, expected_top_oil_temp_c
from app.main import create_app


class FakeClock:
    """Deterministic controllable UTC clock for stale-data testing."""

    def __init__(self, initial: datetime) -> None:
        self.now = initial.astimezone(timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


def _reading(
    event_id: str,
    recorded_at: datetime,
    load_pct: float = 80.0,
    ambient_temp_c: float = 25.0,
    top_oil_temp_c: float | None = None,
    deviation_c: float = 0.0,
    asset_id: str = "TX-001",
) -> dict[str, object]:
    if top_oil_temp_c is None:
        top_oil_temp_c = round(
            expected_top_oil_temp_c(ambient_temp_c, load_pct) + deviation_c, 4
        )
    return {
        "event_id": event_id,
        "asset_id": asset_id,
        "recorded_at": recorded_at.isoformat(),
        "load_pct": load_pct,
        "ambient_temp_c": ambient_temp_c,
        "top_oil_temp_c": top_oil_temp_c,
    }


def _make_client(
    tmp_path: Path,
    config: HealthConfig | None = None,
    clock: FakeClock | None = None,
) -> tuple[TestClient, Path, FakeClock]:
    db_path = tmp_path / "phase2_test.db"
    resolved_clock = clock or FakeClock(datetime(2026, 1, 1, 0, 10, 0, tzinfo=timezone.utc))
    resolved_config = config or HealthConfig(
        deviation_threshold_c=8.0,
        required_consecutive_readings=3,
        recovery_consecutive_readings=2,
        stale_after_seconds=300.0,
        fixed_temp_demo_limit_c=85.0,
    )
    app = create_app(db_path=db_path, config=resolved_config, clock=resolved_clock)
    return TestClient(app), db_path, resolved_clock


def test_no_data_status_before_any_telemetry(tmp_path: Path) -> None:
    client, _, _ = _make_client(tmp_path)
    with client:
        res = client.get("/assets/TX-001/health")
        assert res.status_code == 200
        body = res.json()
        assert body["status"] == "No data"
        assert body["is_stale"] is False
        assert body["has_open_alert"] is False
        assert body["observed_top_oil_temp_c"] is None
        assert body["expected_top_oil_temp_c"] is None
        assert body["deviation_c"] is None
        assert body["latest_reading_at"] is None


def test_normal_load_rise_stays_normal_with_no_alert(tmp_path: Path) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0 + timedelta(minutes=3))
    client, _, _ = _make_client(tmp_path, clock=clock)

    with client:
        for i, load in enumerate([70.0, 100.0, 130.0]):
            assert (
                client.post(
                    "/telemetry",
                    json=_reading(f"evt-load-{i}", t0 + timedelta(minutes=i), load_pct=load),
                ).status_code
                == 201
            )

        health = client.get("/assets/TX-001/health").json()
        assert health["status"] == "Normal"
        assert health["has_open_alert"] is False
        assert abs(health["deviation_c"]) < 0.01
        assert client.get("/assets/TX-001/alerts").json()["count"] == 0


def test_single_spike_resets_on_normal_reading(tmp_path: Path) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0 + timedelta(minutes=3))
    client, _, _ = _make_client(tmp_path, clock=clock)

    with client:
        assert client.post("/telemetry", json=_reading("e1", t0, deviation_c=0.5)).status_code == 201
        assert (
            client.post(
                "/telemetry", json=_reading("e2", t0 + timedelta(minutes=1), deviation_c=12.0)
            ).status_code
            == 201
        )
        h_spike = client.get("/assets/TX-001/health").json()
        assert h_spike["status"] == "Normal"
        assert h_spike["consecutive_abnormal_count"] == 1

        assert (
            client.post(
                "/telemetry", json=_reading("e3", t0 + timedelta(minutes=2), deviation_c=0.5)
            ).status_code
            == 201
        )
        h_after = client.get("/assets/TX-001/health").json()
        assert h_after["status"] == "Normal"
        assert h_after["consecutive_abnormal_count"] == 0
        assert client.get("/assets/TX-001/alerts").json()["count"] == 0


def test_sustained_deviation_continued_degradation_and_recovery(tmp_path: Path) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0 + timedelta(minutes=6))
    client, _, _ = _make_client(tmp_path, clock=clock)

    with client:
        # 3 consecutive abnormal readings below fixed 85 C demo limit (ambient=25, load=50 -> expected ~29.8 C)
        for i in range(3):
            res = client.post(
                "/telemetry",
                json=_reading(
                    f"abn-{i}",
                    t0 + timedelta(minutes=i),
                    load_pct=50.0,
                    ambient_temp_c=25.0,
                    deviation_c=10.0,
                ),
            )
            assert res.status_code == 201

        h_deg = client.get("/assets/TX-001/health").json()
        assert h_deg["status"] == "Degraded"
        assert h_deg["has_open_alert"] is True
        assert h_deg["exceeds_fixed_temp_demo_limit"] is False
        assert h_deg["observed_top_oil_temp_c"] < 85.0

        # Continued degradation must NOT create a duplicate alert
        for i in range(3, 5):
            assert (
                client.post(
                    "/telemetry",
                    json=_reading(
                        f"abn-{i}",
                        t0 + timedelta(minutes=i),
                        load_pct=50.0,
                        ambient_temp_c=25.0,
                        deviation_c=14.0,
                    ),
                ).status_code
                == 201
            )

        alerts = client.get("/assets/TX-001/alerts").json()
        assert alerts["count"] == 1
        assert alerts["items"][0]["status"] == "open"
        assert alerts["items"][0]["peak_deviation_c"] == 14.0

        # 1 normal reading is not enough when recovery_consecutive_readings=2
        assert (
            client.post(
                "/telemetry",
                json=_reading("rec-1", t0 + timedelta(minutes=5), load_pct=50.0, deviation_c=1.0),
            ).status_code
            == 201
        )
        assert client.get("/assets/TX-001/health").json()["status"] == "Degraded"

        # 2nd consecutive normal reading recovers the open alert
        assert (
            client.post(
                "/telemetry",
                json=_reading("rec-2", t0 + timedelta(minutes=6), load_pct=50.0, deviation_c=0.5),
            ).status_code
            == 201
        )
        h_rec = client.get("/assets/TX-001/health").json()
        assert h_rec["status"] == "Normal"
        assert h_rec["has_open_alert"] is False

        alerts_after = client.get("/assets/TX-001/alerts").json()
        assert alerts_after["count"] == 1
        assert alerts_after["items"][0]["status"] == "recovered"
        assert alerts_after["items"][0]["recovered_event_id"] == "rec-2"
        assert alerts_after["items"][0]["recovered_at"] is not None
        assert alerts_after["items"][0]["recovered_reason"] is not None


def test_stale_data_does_not_silently_resolve_open_alert(tmp_path: Path) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0 + timedelta(minutes=3))
    client, _, _ = _make_client(tmp_path, clock=clock)

    with client:
        for i in range(3):
            assert (
                client.post(
                    "/telemetry",
                    json=_reading(f"abn-{i}", t0 + timedelta(minutes=i), deviation_c=11.0),
                ).status_code
                == 201
            )

        assert client.get("/assets/TX-001/health").json()["status"] == "Degraded"

        # Advance clock past stale_after_seconds (300s) from latest reading (t0 + 2m)
        clock.advance(seconds=400)
        h_stale = client.get("/assets/TX-001/health").json()
        assert h_stale["status"] == "Stale"
        assert h_stale["is_stale"] is True
        assert h_stale["has_open_alert"] is True

        open_alerts = client.get("/assets/TX-001/alerts?status=open").json()
        assert open_alerts["count"] == 1
        assert open_alerts["items"][0]["status"] == "open"


def test_exact_retries_do_not_advance_detection_counter(tmp_path: Path) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0 + timedelta(minutes=1))
    client, _, _ = _make_client(tmp_path, clock=clock)

    payload = _reading("retry-evt", t0, deviation_c=12.0)
    with client:
        assert client.post("/telemetry", json=payload).status_code == 201
        for _ in range(3):
            assert client.post("/telemetry", json=payload).status_code == 200

        health = client.get("/assets/TX-001/health").json()
        assert health["consecutive_abnormal_count"] == 1
        assert health["status"] == "Normal"
        assert client.get("/assets/TX-001/alerts").json()["count"] == 0


def test_late_and_equal_timestamp_readings_do_not_rewind_or_advance_health(
    tmp_path: Path,
) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0 + timedelta(minutes=5))
    client, _, _ = _make_client(tmp_path, clock=clock)

    with client:
        assert (
            client.post(
                "/telemetry",
                json=_reading("fresh-1", t0 + timedelta(minutes=4), deviation_c=1.0),
            ).status_code
            == 201
        )
        # Late abnormal reading (earlier timestamp) -> stored in history, ignored for health
        assert (
            client.post(
                "/telemetry",
                json=_reading("late-1", t0 + timedelta(minutes=1), deviation_c=20.0),
            ).status_code
            == 201
        )
        # Equal-timestamp abnormal reading -> stored in history, does not double-count instant
        assert (
            client.post(
                "/telemetry",
                json=_reading("equal-1", t0 + timedelta(minutes=4), deviation_c=20.0),
            ).status_code
            == 201
        )

        health = client.get("/assets/TX-001/health").json()
        assert health["status"] == "Normal"
        assert health["consecutive_abnormal_count"] == 0
        assert health["latest_reading_at"] == "2026-01-01T00:04:00Z"

        history = client.get("/assets/TX-001/history").json()
        assert history["count"] == 3


def test_concurrent_ingestion_around_alert_boundary_opens_single_alert(
    tmp_path: Path,
) -> None:
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0 + timedelta(minutes=10))
    client, db_path, _ = _make_client(tmp_path, clock=clock)

    with client:
        for i in range(2):
            assert (
                client.post(
                    "/telemetry",
                    json=_reading(f"pre-{i}", t0 + timedelta(minutes=i), deviation_c=12.0),
                ).status_code
                == 201
            )

        barrier = threading.Barrier(2)

        def send_boundary(idx: int) -> int:
            barrier.wait(timeout=5.0)
            res = client.post(
                "/telemetry",
                json=_reading(
                    f"boundary-{idx}",
                    t0 + timedelta(minutes=2 + idx),
                    deviation_c=12.0 + idx,
                ),
            )
            return res.status_code

        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = list(pool.map(send_boundary, range(2)))

        assert statuses == [201, 201]

        conn = sqlite3.connect(db_path)
        try:
            open_count = conn.execute(
                "SELECT COUNT(*) FROM alerts WHERE asset_id = 'TX-001' AND status = 'open';"
            ).fetchone()[0]
        finally:
            conn.close()

        assert open_count == 1
        assert client.get("/assets/TX-001/alerts").json()["count"] == 1


def test_open_alert_survives_restart_without_duplication(tmp_path: Path) -> None:
    db_path = tmp_path / "restart_episode.db"
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0 + timedelta(minutes=4))
    config = HealthConfig(
        deviation_threshold_c=8.0,
        required_consecutive_readings=3,
        recovery_consecutive_readings=2,
        stale_after_seconds=300.0,
        fixed_temp_demo_limit_c=85.0,
    )

    app1 = create_app(db_path=db_path, config=config, clock=clock)
    with TestClient(app1) as c1:
        for i in range(3):
            assert (
                c1.post(
                    "/telemetry",
                    json=_reading(f"ep-{i}", t0 + timedelta(minutes=i), deviation_c=10.0),
                ).status_code
                == 201
            )
        assert c1.get("/assets/TX-001/health").json()["status"] == "Degraded"

    app2 = create_app(db_path=db_path, config=config, clock=clock)
    with TestClient(app2) as c2:
        assert (
            c2.post(
                "/telemetry",
                json=_reading("ep-3", t0 + timedelta(minutes=3), deviation_c=13.0),
            ).status_code
            == 201
        )
        health = c2.get("/assets/TX-001/health").json()
        assert health["status"] == "Degraded"
        assert health["has_open_alert"] is True

        alerts = c2.get("/assets/TX-001/alerts").json()
        assert alerts["count"] == 1
        assert alerts["items"][0]["status"] == "open"
        assert alerts["items"][0]["peak_deviation_c"] == 13.0
