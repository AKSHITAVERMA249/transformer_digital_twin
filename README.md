# Transformer Digital Twin — Phase 1

Phase 1 prototype for power transformer `TX-001`: deterministic HTTP telemetry simulator → FastAPI ingestion API → file-backed SQLite persistence → history API.

## Persistence & Design Choices

- **SQLite (file-backed, WAL mode)**: Chosen for the default local demo so the entire stack runs with Python's standard library `sqlite3` module without requiring Docker or an external database server. By default, data is stored in `data/telemetry.db` (configurable via `TELEMETRY_DB_PATH`).
- **Database-enforced idempotency**: `event_id` has a `UNIQUE` constraint in SQLite. `POST /telemetry` returns `201` for a new event, `200` for an exact normalized retry (including equivalent timezone offsets normalized to UTC), and `409` if the same `event_id` is sent with different data.
- **Validation**: `recorded_at` must be timezone-aware and is normalized to UTC. Measurements (`load_pct`, `ambient_temp_c`, `top_oil_temp_c`) must be finite numbers. `load_pct` is not capped at 100% so overload scenarios are supported.
- **Deterministic history ordering**: `GET /assets/{asset_id}/history` accepts `limit` (default `100`, bounded `1..1000`) and sorts deterministically by `recorded_at DESC, id DESC` (newest first, with insertion `id` as tie-breaker).

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the Server & Dashboard

```bash
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open the live dashboard in your browser at:

```text
http://127.0.0.1:8000/
```

## Run the Simulator

Send 12 deterministic readings for `TX-001` (including an overload pulse above 100% load) and exit:

```bash
python simulator.py --steps 12 --interval 0.1
```

Use `--steps 0` for continuous streaming.

## Fetch History, Health, and Alerts

```bash
curl "http://127.0.0.1:8000/assets/TX-001/history?limit=10"
curl "http://127.0.0.1:8000/assets/TX-001/health"
curl "http://127.0.0.1:8000/assets/TX-001/alerts?limit=10"
```

## Phase 2: Health Baseline & Alerting (Demo Settings)

- **Expected top-oil temperature formula**:
  `expected_top_oil_temp_c = ambient_temp_c + 22.0 * (max(load_pct, 0.0) / 100.0) ** 1.6`
  (temperatures in `°C`, `load_pct` in `%` of rated load).
- **Assumptions & limitations**: Steady-state algebraic approximation with no thermal lag / time constant and uncalibrated demo constants. All thresholds below are **demo settings only**, not utility operating limits:
  - `TWIN_DEVIATION_THRESHOLD_C` (default `8.0` °C): positive `observed - expected` deviation threshold.
  - `TWIN_REQUIRED_CONSECUTIVE_READINGS` (default `3`): consecutive fresh, in-order abnormal readings to open an alert.
  - `TWIN_RECOVERY_CONSECUTIVE_READINGS` (default `2`): consecutive fresh, in-order normal readings (`deviation_c < deviation_threshold_c`) to mark an open alert `recovered`.
  - `TWIN_STALE_AFTER_SECONDS` (default `300.0` s): age after which health status becomes `Stale` (without silently resolving any open alert).
  - `TWIN_FIXED_TEMP_DEMO_LIMIT_C` (default `85.0` °C): separate fixed-temperature demo comparator reported alongside model-based health.
- **Ordering & idempotency**: Telemetry, `asset_health_state`, and `alerts` update atomically in one SQLite transaction. Only strictly newer timestamps (`recorded_at > latest_recorded_at`) advance health state; late (`<`) and equal (`==`) timestamps are stored in history without rewinding health or double-counting. Exact retries never advance counters.

## Run Tests

```bash
pytest -v
```
