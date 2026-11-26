from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import math
import os
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse

from app.db import (
    ConflictError,
    DEFAULT_DB_PATH,
    get_asset_alerts,
    get_asset_health,
    get_asset_history,
    ingest_telemetry,
    init_db,
)
from app.health import HealthConfig
from app.schemas import (
    AlertHistoryResponse,
    AssetHealthResponse,
    HistoryResponse,
    TelemetryIn,
    TelemetryIngestResponse,
)

MAX_HISTORY_LIMIT = 1000
DEFAULT_HISTORY_LIMIT = 100
HISTORY_SORT_ORDER = "recorded_at DESC, id DESC"
ALERTS_SORT_ORDER = "opened_at DESC, id DESC"
DASHBOARD_HTML_PATH = Path(__file__).resolve().parent / "static" / "index.html"


def get_db_path() -> Path:
    return Path(os.environ.get("TELEMETRY_DB_PATH", str(DEFAULT_DB_PATH)))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def create_app(
    db_path: Path | None = None,
    config: HealthConfig | None = None,
    clock: Callable[[], datetime] | None = None,
) -> FastAPI:
    resolved_db_path = db_path if db_path is not None else get_db_path()
    resolved_config = config if config is not None else HealthConfig.from_env()
    resolved_clock = clock if clock is not None else _utc_now

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        init_db(resolved_db_path)
        yield

    app = FastAPI(
        title="Transformer Digital Twin - Phase 1 Telemetry API",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.db_path = resolved_db_path
    app.state.health_config = resolved_config
    app.state.clock = resolved_clock

    def _sanitize_json_value(value: object) -> object:
        if isinstance(value, float) and not math.isfinite(value):
            return str(value)
        if isinstance(value, dict):
            return {k: _sanitize_json_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_sanitize_json_value(v) for v in value]
        if isinstance(value, tuple):
            return [_sanitize_json_value(v) for v in value]
        return value

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={"detail": jsonable_encoder(_sanitize_json_value(exc.errors()))},
        )

    @app.get("/", include_in_schema=False)
    @app.get("/dashboard", include_in_schema=False)
    def dashboard() -> FileResponse:
        return FileResponse(DASHBOARD_HTML_PATH, media_type="text/html")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/telemetry",
        response_model=TelemetryIngestResponse,
        status_code=status.HTTP_201_CREATED,
    )
    def post_telemetry(
        payload: TelemetryIn, response: Response
    ) -> TelemetryIngestResponse:
        try:
            outcome, record = ingest_telemetry(
                app.state.db_path, payload, config=app.state.health_config
            )
        except ConflictError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc

        if outcome == "duplicate":
            response.status_code = status.HTTP_200_OK
        else:
            response.status_code = status.HTTP_201_CREATED

        return TelemetryIngestResponse(status=outcome, record=record)

    @app.get(
        "/assets/{asset_id}/history",
        response_model=HistoryResponse,
    )
    def read_asset_history(
        asset_id: str,
        limit: int = Query(
            default=DEFAULT_HISTORY_LIMIT,
            ge=1,
            le=MAX_HISTORY_LIMIT,
            description=f"Maximum number of records to return (1..{MAX_HISTORY_LIMIT})",
        ),
    ) -> HistoryResponse:
        items = get_asset_history(app.state.db_path, asset_id=asset_id, limit=limit)
        return HistoryResponse(
            asset_id=asset_id,
            limit=limit,
            sort_order=HISTORY_SORT_ORDER,
            count=len(items),
            items=items,
        )

    @app.get(
        "/assets/{asset_id}/health",
        response_model=AssetHealthResponse,
    )
    def read_asset_health(asset_id: str) -> AssetHealthResponse:
        return get_asset_health(
            app.state.db_path,
            asset_id=asset_id,
            config=app.state.health_config,
            now=app.state.clock(),
        )

    @app.get(
        "/assets/{asset_id}/alerts",
        response_model=AlertHistoryResponse,
    )
    def read_asset_alerts(
        asset_id: str,
        limit: int = Query(
            default=DEFAULT_HISTORY_LIMIT,
            ge=1,
            le=MAX_HISTORY_LIMIT,
            description=f"Maximum number of alerts to return (1..{MAX_HISTORY_LIMIT})",
        ),
        status_filter: Literal["open", "recovered"] | None = Query(
            default=None,
            alias="status",
            description="Optional filter by alert status ('open' or 'recovered')",
        ),
    ) -> AlertHistoryResponse:
        items = get_asset_alerts(
            app.state.db_path,
            asset_id=asset_id,
            limit=limit,
            status_filter=status_filter,
        )
        return AlertHistoryResponse(
            asset_id=asset_id,
            limit=limit,
            sort_order=ALERTS_SORT_ORDER,
            count=len(items),
            items=items,
        )

    return app


app = create_app()
