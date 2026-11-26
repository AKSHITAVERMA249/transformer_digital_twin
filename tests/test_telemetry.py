from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import threading

from fastapi.testclient import TestClient
import pytest

from app.main import create_app


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "telemetry_test.db"


@pytest.fixture()
def client(db_path: Path):
    app = create_app(db_path=db_path)
    with TestClient(app) as test_client:
        yield test_client


def _sample_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_id": "evt-001",
        "asset_id": "TX-001",
        "recorded_at": "2026-01-01T05:30:00+05:30",
        "load_pct": 112.5,
        "ambient_temp_c": 29.4,
        "top_oil_temp_c": 61.8,
    }
    payload.update(overrides)
    return payload


def _count_rows(db_path: Path, event_id: str | None = None) -> int:
    conn = sqlite3.connect(db_path)
    try:
        if event_id is None:
            row = conn.execute("SELECT COUNT(*) FROM telemetry;").fetchone()
        else:
            row = conn.execute(
                "SELECT COUNT(*) FROM telemetry WHERE event_id = ?;", (event_id,)
            ).fetchone()
        return int(row[0])
    finally:
        conn.close()


def test_valid_ingestion_normalizes_utc_and_allows_overload(
    client: TestClient, db_path: Path
) -> None:
    response = client.post("/telemetry", json=_sample_payload(load_pct=135.0))
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "created"
    assert body["record"]["event_id"] == "evt-001"
    assert body["record"]["asset_id"] == "TX-001"
    assert body["record"]["load_pct"] == 135.0
    assert body["record"]["recorded_at"] == "2026-01-01T00:00:00Z"
    assert _count_rows(db_path) == 1


def test_exact_retry_returns_200_with_no_extra_row(
    client: TestClient, db_path: Path
) -> None:
    first = client.post("/telemetry", json=_sample_payload())
    assert first.status_code == 201

    # Same instant expressed in UTC instead of +05:30
    retry_payload = _sample_payload(recorded_at="2026-01-01T00:00:00Z")
    second = client.post("/telemetry", json=retry_payload)
    assert second.status_code == 200
    assert second.json()["status"] == "duplicate"
    assert second.json()["record"] == first.json()["record"]
    assert _count_rows(db_path) == 1


def test_conflicting_retry_returns_409_and_preserves_original(
    client: TestClient, db_path: Path
) -> None:
    first = client.post("/telemetry", json=_sample_payload(load_pct=95.0))
    assert first.status_code == 201

    conflict = client.post("/telemetry", json=_sample_payload(load_pct=96.0))
    assert conflict.status_code == 409
    assert _count_rows(db_path) == 1

    history = client.get("/assets/TX-001/history")
    assert history.status_code == 200
    assert history.json()["items"][0]["load_pct"] == 95.0


@pytest.mark.parametrize(
    "missing_field",
    [
        "event_id",
        "asset_id",
        "recorded_at",
        "load_pct",
        "ambient_temp_c",
        "top_oil_temp_c",
    ],
)
def test_missing_fields_rejected(client: TestClient, missing_field: str) -> None:
    payload = _sample_payload()
    payload.pop(missing_field)
    response = client.post("/telemetry", json=payload)
    assert response.status_code == 422


def test_timezone_naive_timestamp_rejected(client: TestClient) -> None:
    response = client.post(
        "/telemetry",
        json=_sample_payload(recorded_at="2026-01-01T00:00:00"),
    )
    assert response.status_code == 422


@pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("field_name", ["load_pct", "ambient_temp_c", "top_oil_temp_c"])
def test_non_finite_measurements_rejected(
    client: TestClient, field_name: str, bad_value: str
) -> None:
    raw_json = (
        '{"event_id": "evt-bad", "asset_id": "TX-001", '
        '"recorded_at": "2026-01-01T00:00:00Z", '
        '"load_pct": 80.0, "ambient_temp_c": 25.0, "top_oil_temp_c": 55.0}'
    )
    default_val = {"load_pct": "80.0", "ambient_temp_c": "25.0", "top_oil_temp_c": "55.0"}[
        field_name
    ]
    raw_json = raw_json.replace(f'"{field_name}": {default_val}', f'"{field_name}": {bad_value}')

    response = client.post(
        "/telemetry",
        content=raw_json,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


def test_deterministic_history_ordering_and_bounded_limit(client: TestClient) -> None:
    events = [
        _sample_payload(event_id="evt-t1", recorded_at="2026-01-01T00:01:00Z"),
        _sample_payload(event_id="evt-t3-a", recorded_at="2026-01-01T00:03:00Z"),
        _sample_payload(event_id="evt-t2", recorded_at="2026-01-01T00:02:00Z"),
        _sample_payload(event_id="evt-t3-b", recorded_at="2026-01-01T00:03:00Z"),
    ]
    for event in events:
        res = client.post("/telemetry", json=event)
        assert res.status_code == 201

    full = client.get("/assets/TX-001/history?limit=10")
    assert full.status_code == 200
    data = full.json()
    assert data["sort_order"] == "recorded_at DESC, id DESC"
    assert [item["event_id"] for item in data["items"]] == [
        "evt-t3-b",
        "evt-t3-a",
        "evt-t2",
        "evt-t1",
    ]

    limited = client.get("/assets/TX-001/history?limit=2")
    assert limited.status_code == 200
    assert [item["event_id"] for item in limited.json()["items"]] == [
        "evt-t3-b",
        "evt-t3-a",
    ]

    assert client.get("/assets/TX-001/history?limit=0").status_code == 422
    assert client.get("/assets/TX-001/history?limit=1001").status_code == 422


def test_simultaneous_requests_same_event_id_create_single_row(
    client: TestClient, db_path: Path
) -> None:
    payload = _sample_payload(event_id="evt-concurrent-1")
    barrier = threading.Barrier(2)

    def send_once() -> int:
        barrier.wait(timeout=5.0)
        res = client.post("/telemetry", json=payload)
        return res.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = list(pool.map(lambda _: send_once(), range(2)))

    assert sorted(statuses) == [200, 201]
    assert _count_rows(db_path, event_id="evt-concurrent-1") == 1


def test_database_enforces_event_id_uniqueness(
    client: TestClient, db_path: Path
) -> None:
    assert client.post("/telemetry", json=_sample_payload(event_id="evt-db-unique")).status_code == 201

    conn = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO telemetry (
                    event_id, asset_id, recorded_at, load_pct, ambient_temp_c, top_oil_temp_c, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    "evt-db-unique",
                    "TX-001",
                    "2026-01-01T00:00:00+00:00",
                    80.0,
                    25.0,
                    50.0,
                    "2026-01-01T00:00:00+00:00",
                ),
            )
    finally:
        conn.close()


def test_data_survives_app_restart(db_path: Path) -> None:
    app1 = create_app(db_path=db_path)
    with TestClient(app1) as client1:
        res = client1.post("/telemetry", json=_sample_payload(event_id="evt-persist-1"))
        assert res.status_code == 201

    app2 = create_app(db_path=db_path)
    with TestClient(app2) as client2:
        history = client2.get("/assets/TX-001/history")
        assert history.status_code == 200
        items = history.json()["items"]
        assert len(items) == 1
        assert items[0]["event_id"] == "evt-persist-1"
