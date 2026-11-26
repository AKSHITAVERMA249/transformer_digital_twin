from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Iterator, Literal

from app.health import (
    BASELINE_FORMULA_NOTE,
    HealthConfig,
    compute_deviation_c,
    expected_top_oil_temp_c,
)
from app.schemas import (
    AlertRecord,
    AssetHealthResponse,
    TelemetryIn,
    TelemetryRecord,
)

DEFAULT_DB_PATH = Path("data/telemetry.db")


class ConflictError(Exception):
    """Raised when an existing event_id is retried with a different normalized payload."""


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


@contextmanager
def get_connection(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = _connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def init_db(db_path: Path) -> None:
    """Create telemetry table and history index if they do not exist."""
    with get_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                asset_id TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                load_pct REAL NOT NULL,
                ambient_temp_c REAL NOT NULL,
                top_oil_temp_c REAL NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telemetry_asset_history
            ON telemetry (asset_id, recorded_at DESC, id DESC);
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS asset_health_state (
                asset_id TEXT PRIMARY KEY,
                latest_recorded_at TEXT NOT NULL,
                latest_telemetry_id INTEGER NOT NULL,
                latest_event_id TEXT NOT NULL,
                observed_top_oil_temp_c REAL NOT NULL,
                expected_top_oil_temp_c REAL NOT NULL,
                deviation_c REAL NOT NULL,
                load_pct REAL NOT NULL,
                ambient_temp_c REAL NOT NULL,
                consecutive_abnormal_count INTEGER NOT NULL,
                consecutive_normal_count INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('open', 'recovered')),
                opened_at TEXT NOT NULL,
                opened_reason TEXT NOT NULL,
                opened_event_id TEXT NOT NULL,
                peak_deviation_c REAL NOT NULL,
                recovered_at TEXT,
                recovered_reason TEXT,
                recovered_event_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_one_open_per_asset
            ON alerts (asset_id)
            WHERE status = 'open';
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_alerts_asset_history
            ON alerts (asset_id, opened_at DESC, id DESC);
            """
        )


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _row_to_record(row: sqlite3.Row) -> TelemetryRecord:
    return TelemetryRecord(
        id=row["id"],
        event_id=row["event_id"],
        asset_id=row["asset_id"],
        recorded_at=datetime.fromisoformat(row["recorded_at"]).astimezone(timezone.utc),
        load_pct=float(row["load_pct"]),
        ambient_temp_c=float(row["ambient_temp_c"]),
        top_oil_temp_c=float(row["top_oil_temp_c"]),
    )


def _payload_matches(existing: TelemetryRecord, incoming: TelemetryIn) -> bool:
    return (
        existing.asset_id == incoming.asset_id
        and existing.recorded_at == incoming.recorded_at
        and existing.load_pct == incoming.load_pct
        and existing.ambient_temp_c == incoming.ambient_temp_c
        and existing.top_oil_temp_c == incoming.top_oil_temp_c
    )


def _update_health_and_alerts(
    conn: sqlite3.Connection,
    payload: TelemetryIn,
    telemetry_id: int,
    recorded_at_iso: str,
    now_iso: str,
    config: HealthConfig,
) -> None:
    """
    Update asset_health_state and alerts atomically inside the active transaction.

    Ordering rule:
    - Strictly newer timestamps (`recorded_at > latest_recorded_at`) advance health state.
    - Late timestamps (`recorded_at < latest_recorded_at`) and equal timestamps
      (`recorded_at == latest_recorded_at`) are stored in telemetry history only so they
      never rewind current health or double-count the same instant.
    """
    state_row = conn.execute(
        """
        SELECT latest_recorded_at, consecutive_abnormal_count, consecutive_normal_count
        FROM asset_health_state
        WHERE asset_id = ?;
        """,
        (payload.asset_id,),
    ).fetchone()

    if state_row is not None:
        latest_dt = datetime.fromisoformat(state_row["latest_recorded_at"]).astimezone(
            timezone.utc
        )
        if payload.recorded_at <= latest_dt:
            return
        prev_abnormal = int(state_row["consecutive_abnormal_count"])
        prev_normal = int(state_row["consecutive_normal_count"])
    else:
        prev_abnormal = 0
        prev_normal = 0

    expected_c = expected_top_oil_temp_c(payload.ambient_temp_c, payload.load_pct)
    deviation_c = compute_deviation_c(payload.top_oil_temp_c, expected_c)
    is_abnormal = deviation_c >= config.deviation_threshold_c

    if is_abnormal:
        abnormal_count = prev_abnormal + 1
        normal_count = 0
    else:
        abnormal_count = 0
        normal_count = prev_normal + 1

    open_alert = conn.execute(
        """
        SELECT id, peak_deviation_c
        FROM alerts
        WHERE asset_id = ? AND status = 'open';
        """,
        (payload.asset_id,),
    ).fetchone()

    if open_alert is None:
        if is_abnormal and abnormal_count >= config.required_consecutive_readings:
            reason = (
                f"Sustained positive top-oil deviation: {abnormal_count} consecutive "
                f"readings >= {config.deviation_threshold_c:.2f} C demo threshold "
                f"(observed={payload.top_oil_temp_c:.2f} C, expected={expected_c:.2f} C, "
                f"deviation={deviation_c:+.2f} C)"
            )
            conn.execute(
                """
                INSERT INTO alerts (
                    asset_id, status, opened_at, opened_reason, opened_event_id,
                    peak_deviation_c, created_at, updated_at
                ) VALUES (?, 'open', ?, ?, ?, ?, ?, ?);
                """,
                (
                    payload.asset_id,
                    recorded_at_iso,
                    reason,
                    payload.event_id,
                    deviation_c,
                    now_iso,
                    now_iso,
                ),
            )
    else:
        alert_id = int(open_alert["id"])
        if is_abnormal:
            peak_dev = max(float(open_alert["peak_deviation_c"]), deviation_c)
            conn.execute(
                """
                UPDATE alerts
                SET peak_deviation_c = ?, updated_at = ?
                WHERE id = ?;
                """,
                (peak_dev, now_iso, alert_id),
            )
        elif normal_count >= config.recovery_consecutive_readings:
            recovered_reason = (
                f"Recovered after {normal_count} consecutive readings below "
                f"{config.deviation_threshold_c:.2f} C demo threshold "
                f"(observed={payload.top_oil_temp_c:.2f} C, expected={expected_c:.2f} C, "
                f"deviation={deviation_c:+.2f} C)"
            )
            conn.execute(
                """
                UPDATE alerts
                SET status = 'recovered',
                    recovered_at = ?,
                    recovered_reason = ?,
                    recovered_event_id = ?,
                    updated_at = ?
                WHERE id = ?;
                """,
                (
                    recorded_at_iso,
                    recovered_reason,
                    payload.event_id,
                    now_iso,
                    alert_id,
                ),
            )

    conn.execute(
        """
        INSERT INTO asset_health_state (
            asset_id, latest_recorded_at, latest_telemetry_id, latest_event_id,
            observed_top_oil_temp_c, expected_top_oil_temp_c, deviation_c,
            load_pct, ambient_temp_c, consecutive_abnormal_count,
            consecutive_normal_count, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(asset_id) DO UPDATE SET
            latest_recorded_at = excluded.latest_recorded_at,
            latest_telemetry_id = excluded.latest_telemetry_id,
            latest_event_id = excluded.latest_event_id,
            observed_top_oil_temp_c = excluded.observed_top_oil_temp_c,
            expected_top_oil_temp_c = excluded.expected_top_oil_temp_c,
            deviation_c = excluded.deviation_c,
            load_pct = excluded.load_pct,
            ambient_temp_c = excluded.ambient_temp_c,
            consecutive_abnormal_count = excluded.consecutive_abnormal_count,
            consecutive_normal_count = excluded.consecutive_normal_count,
            updated_at = excluded.updated_at;
        """,
        (
            payload.asset_id,
            recorded_at_iso,
            telemetry_id,
            payload.event_id,
            payload.top_oil_temp_c,
            expected_c,
            deviation_c,
            payload.load_pct,
            payload.ambient_temp_c,
            abnormal_count,
            normal_count,
            now_iso,
        ),
    )


def ingest_telemetry(
    db_path: Path,
    payload: TelemetryIn,
    config: HealthConfig | None = None,
) -> tuple[Literal["created", "duplicate"], TelemetryRecord]:
    """
    Insert a telemetry reading and atomically update health state & alerts,
    or reconcile an idempotent/conflicting retry.

    Uniqueness of `event_id` is enforced by the SQLite UNIQUE constraint.
    """
    resolved_config = config if config is not None else HealthConfig()
    recorded_at_iso = _format_utc(payload.recorded_at)
    created_at_iso = _format_utc(datetime.now(timezone.utc))

    with get_connection(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE;")
            cursor = conn.execute(
                """
                INSERT INTO telemetry (
                    event_id,
                    asset_id,
                    recorded_at,
                    load_pct,
                    ambient_temp_c,
                    top_oil_temp_c,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    payload.event_id,
                    payload.asset_id,
                    recorded_at_iso,
                    payload.load_pct,
                    payload.ambient_temp_c,
                    payload.top_oil_temp_c,
                    created_at_iso,
                ),
            )
            row_id = cursor.lastrowid
            assert row_id is not None
            _update_health_and_alerts(
                conn=conn,
                payload=payload,
                telemetry_id=int(row_id),
                recorded_at_iso=recorded_at_iso,
                now_iso=created_at_iso,
                config=resolved_config,
            )
            row = conn.execute(
                """
                SELECT id, event_id, asset_id, recorded_at, load_pct, ambient_temp_c, top_oil_temp_c
                FROM telemetry
                WHERE id = ?;
                """,
                (row_id,),
            ).fetchone()
            conn.execute("COMMIT;")
            assert row is not None
            return "created", _row_to_record(row)
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK;")
            existing_row = conn.execute(
                """
                SELECT id, event_id, asset_id, recorded_at, load_pct, ambient_temp_c, top_oil_temp_c
                FROM telemetry
                WHERE event_id = ?;
                """,
                (payload.event_id,),
            ).fetchone()
            if existing_row is None:
                raise
            existing = _row_to_record(existing_row)
            if _payload_matches(existing, payload):
                return "duplicate", existing
            raise ConflictError(
                f"event_id '{payload.event_id}' already exists with different telemetry data"
            )
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK;")
            raise


def get_asset_history(db_path: Path, asset_id: str, limit: int) -> list[TelemetryRecord]:
    """
    Return up to `limit` telemetry readings for `asset_id` ordered deterministically
    by `recorded_at DESC, id DESC` (newest first, with insertion ID tie-breaker).
    """
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, event_id, asset_id, recorded_at, load_pct, ambient_temp_c, top_oil_temp_c
            FROM telemetry
            WHERE asset_id = ?
            ORDER BY recorded_at DESC, id DESC
            LIMIT ?;
            """,
            (asset_id, limit),
        ).fetchall()
    return [_row_to_record(row) for row in rows]


def get_asset_health(
    db_path: Path,
    asset_id: str,
    config: HealthConfig,
    now: datetime,
) -> AssetHealthResponse:
    """Evaluate current health for `asset_id` without silently resolving open alerts on staleness."""
    evaluated_at = now.astimezone(timezone.utc)
    with get_connection(db_path) as conn:
        state_row = conn.execute(
            """
            SELECT *
            FROM asset_health_state
            WHERE asset_id = ?;
            """,
            (asset_id,),
        ).fetchone()
        open_alert_row = conn.execute(
            """
            SELECT id
            FROM alerts
            WHERE asset_id = ? AND status = 'open';
            """,
            (asset_id,),
        ).fetchone()

    has_open_alert = open_alert_row is not None

    if state_row is None:
        return AssetHealthResponse(
            asset_id=asset_id,
            status="No data",
            is_stale=False,
            has_open_alert=has_open_alert,
            observed_top_oil_temp_c=None,
            expected_top_oil_temp_c=None,
            deviation_c=None,
            load_pct=None,
            ambient_temp_c=None,
            latest_reading_at=None,
            evaluated_at=evaluated_at,
            consecutive_abnormal_count=0,
            consecutive_normal_count=0,
            deviation_threshold_c=config.deviation_threshold_c,
            required_consecutive_readings=config.required_consecutive_readings,
            recovery_consecutive_readings=config.recovery_consecutive_readings,
            stale_after_seconds=config.stale_after_seconds,
            fixed_temp_demo_limit_c=config.fixed_temp_demo_limit_c,
            exceeds_fixed_temp_demo_limit=None,
            baseline_note=BASELINE_FORMULA_NOTE,
        )

    latest_reading_at = datetime.fromisoformat(state_row["latest_recorded_at"]).astimezone(
        timezone.utc
    )
    age_seconds = (evaluated_at - latest_reading_at).total_seconds()
    is_stale = age_seconds > config.stale_after_seconds

    if is_stale:
        status_label: Literal["No data", "Normal", "Degraded", "Stale"] = "Stale"
    elif has_open_alert:
        status_label = "Degraded"
    else:
        status_label = "Normal"

    observed_temp = float(state_row["observed_top_oil_temp_c"])
    return AssetHealthResponse(
        asset_id=asset_id,
        status=status_label,
        is_stale=is_stale,
        has_open_alert=has_open_alert,
        observed_top_oil_temp_c=observed_temp,
        expected_top_oil_temp_c=float(state_row["expected_top_oil_temp_c"]),
        deviation_c=float(state_row["deviation_c"]),
        load_pct=float(state_row["load_pct"]),
        ambient_temp_c=float(state_row["ambient_temp_c"]),
        latest_reading_at=latest_reading_at,
        evaluated_at=evaluated_at,
        consecutive_abnormal_count=int(state_row["consecutive_abnormal_count"]),
        consecutive_normal_count=int(state_row["consecutive_normal_count"]),
        deviation_threshold_c=config.deviation_threshold_c,
        required_consecutive_readings=config.required_consecutive_readings,
        recovery_consecutive_readings=config.recovery_consecutive_readings,
        stale_after_seconds=config.stale_after_seconds,
        fixed_temp_demo_limit_c=config.fixed_temp_demo_limit_c,
        exceeds_fixed_temp_demo_limit=observed_temp >= config.fixed_temp_demo_limit_c,
        baseline_note=BASELINE_FORMULA_NOTE,
    )


def _row_to_alert(row: sqlite3.Row) -> AlertRecord:
    return AlertRecord(
        id=int(row["id"]),
        asset_id=row["asset_id"],
        status=row["status"],
        opened_at=datetime.fromisoformat(row["opened_at"]).astimezone(timezone.utc),
        opened_reason=row["opened_reason"],
        opened_event_id=row["opened_event_id"],
        peak_deviation_c=float(row["peak_deviation_c"]),
        recovered_at=(
            datetime.fromisoformat(row["recovered_at"]).astimezone(timezone.utc)
            if row["recovered_at"] is not None
            else None
        ),
        recovered_reason=row["recovered_reason"],
        recovered_event_id=row["recovered_event_id"],
    )


def get_asset_alerts(
    db_path: Path,
    asset_id: str,
    limit: int,
    status_filter: Literal["open", "recovered"] | None = None,
) -> list[AlertRecord]:
    """Return alert episodes for `asset_id` ordered deterministically by `opened_at DESC, id DESC`."""
    with get_connection(db_path) as conn:
        if status_filter is None:
            rows = conn.execute(
                """
                SELECT *
                FROM alerts
                WHERE asset_id = ?
                ORDER BY opened_at DESC, id DESC
                LIMIT ?;
                """,
                (asset_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM alerts
                WHERE asset_id = ? AND status = ?
                ORDER BY opened_at DESC, id DESC
                LIMIT ?;
                """,
                (asset_id, status_filter, limit),
            ).fetchall()
    return [_row_to_alert(row) for row in rows]
