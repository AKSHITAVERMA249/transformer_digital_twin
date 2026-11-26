"""Demo thermal baseline and health evaluation configuration for Phase 2."""

from __future__ import annotations

from dataclasses import dataclass
import os

BASELINE_FORMULA_NOTE = (
    "Demo baseline: expected_top_oil_temp_c = ambient_temp_c + 22.0 * (max(load_pct, 0.0) / 100.0) ** 1.6 "
    "(temperatures in deg C, load_pct in % of rated load). "
    "Assumptions: steady-state algebraic approximation with no thermal lag/time constant and uncalibrated demo constants. "
    "All thresholds and fixed-temperature comparators are demo settings only, not utility operating limits."
)


@dataclass(frozen=True)
class HealthConfig:
    """Configurable demo thresholds for transformer health and alert detection."""

    # Demo setting: positive deviation (observed - expected) above which a reading counts as abnormal.
    deviation_threshold_c: float = 8.0
    # Demo setting: consecutive fresh, in-order abnormal readings required to open an alert.
    required_consecutive_readings: int = 3
    # Demo setting: consecutive fresh, in-order normal readings required to recover an open alert.
    recovery_consecutive_readings: int = 2
    # Demo setting: age (seconds) after which the latest reading is considered Stale.
    stale_after_seconds: float = 300.0
    # Demo comparator only (kept separate from model-based deviation detection): fixed top-oil temperature limit.
    fixed_temp_demo_limit_c: float = 85.0

    @classmethod
    def from_env(cls) -> HealthConfig:
        return cls(
            deviation_threshold_c=float(
                os.environ.get("TWIN_DEVIATION_THRESHOLD_C", "8.0")
            ),
            required_consecutive_readings=int(
                os.environ.get("TWIN_REQUIRED_CONSECUTIVE_READINGS", "3")
            ),
            recovery_consecutive_readings=int(
                os.environ.get("TWIN_RECOVERY_CONSECUTIVE_READINGS", "2")
            ),
            stale_after_seconds=float(
                os.environ.get("TWIN_STALE_AFTER_SECONDS", "300.0")
            ),
            fixed_temp_demo_limit_c=float(
                os.environ.get("TWIN_FIXED_TEMP_DEMO_LIMIT_C", "85.0")
            ),
        )


def expected_top_oil_temp_c(ambient_temp_c: float, load_pct: float) -> float:
    """
    Compute steady-state demo baseline top-oil temperature in degrees Celsius.

    Formula:
        expected_top_oil_temp_c = ambient_temp_c + 22.0 * (max(load_pct, 0.0) / 100.0) ** 1.6
    """
    normalized_load = max(load_pct, 0.0) / 100.0
    thermal_rise_c = 22.0 * (normalized_load ** 1.6)
    return round(ambient_temp_c + thermal_rise_c, 4)


def compute_deviation_c(observed_top_oil_temp_c: float, expected_temp_c: float) -> float:
    """Compute positive/negative deviation (observed - expected) in degrees Celsius."""
    return round(observed_top_oil_temp_c - expected_temp_c, 4)
