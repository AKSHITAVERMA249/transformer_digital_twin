from datetime import datetime, timezone
import math
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TelemetryIn(BaseModel):
    """Validated incoming telemetry reading for a power transformer."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(..., min_length=1, description="Unique idempotency key for the event")
    asset_id: str = Field(..., min_length=1, description="Transformer asset identifier")
    recorded_at: datetime = Field(..., description="Timezone-aware timestamp of the reading")
    load_pct: float = Field(..., description="Transformer load percentage (may exceed 100% during overload)")
    ambient_temp_c: float = Field(..., description="Ambient temperature in degrees Celsius")
    top_oil_temp_c: float = Field(..., description="Top-oil temperature in degrees Celsius")

    @field_validator("event_id", "asset_id", mode="before")
    @classmethod
    def strip_non_empty_str(cls, value: object) -> object:
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise ValueError("must not be empty or whitespace")
            return stripped
        return value

    @field_validator("recorded_at")
    @classmethod
    def require_tz_and_normalize_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("recorded_at must be timezone-aware")
        return value.astimezone(timezone.utc)

    @field_validator("load_pct", "ambient_temp_c", "top_oil_temp_c")
    @classmethod
    def require_finite_float(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("measurement must be a finite number")
        return float(value)


class TelemetryRecord(BaseModel):
    """Persisted telemetry reading returned by the API."""

    id: int
    event_id: str
    asset_id: str
    recorded_at: datetime
    load_pct: float
    ambient_temp_c: float
    top_oil_temp_c: float


class TelemetryIngestResponse(BaseModel):
    """Response returned by POST /telemetry."""

    status: Literal["created", "duplicate"]
    record: TelemetryRecord


class HistoryResponse(BaseModel):
    """Response returned by GET /assets/{asset_id}/history."""

    asset_id: str
    limit: int
    sort_order: str
    count: int
    items: list[TelemetryRecord]


class AssetHealthResponse(BaseModel):
    """Current health summary for an asset, ready for future dashboard consumption."""

    asset_id: str
    status: Literal["No data", "Normal", "Degraded", "Stale"]
    is_stale: bool
    has_open_alert: bool
    observed_top_oil_temp_c: float | None
    expected_top_oil_temp_c: float | None
    deviation_c: float | None
    load_pct: float | None
    ambient_temp_c: float | None
    latest_reading_at: datetime | None
    evaluated_at: datetime
    consecutive_abnormal_count: int
    consecutive_normal_count: int
    deviation_threshold_c: float
    required_consecutive_readings: int
    recovery_consecutive_readings: int
    stale_after_seconds: float
    fixed_temp_demo_limit_c: float
    exceeds_fixed_temp_demo_limit: bool | None
    baseline_note: str


class AlertRecord(BaseModel):
    """Persisted anomaly episode record."""

    id: int
    asset_id: str
    status: Literal["open", "recovered"]
    opened_at: datetime
    opened_reason: str
    opened_event_id: str
    peak_deviation_c: float
    recovered_at: datetime | None
    recovered_reason: str | None
    recovered_event_id: str | None


class AlertHistoryResponse(BaseModel):
    """Response returned by GET /assets/{asset_id}/alerts."""

    asset_id: str
    limit: int
    sort_order: str
    count: int
    items: list[AlertRecord]
