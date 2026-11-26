"""Deterministic telemetry simulator for power transformer TX-001."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import math
import sys
import time

import httpx

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_ASSET_ID = "TX-001"
DEFAULT_START_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def generate_reading(
    step: int,
    asset_id: str = DEFAULT_ASSET_ID,
    start_time: datetime = DEFAULT_START_TIME,
    step_seconds: int = 60,
) -> dict[str, object]:
    """
    Generate a deterministic telemetry payload for a given step index.

    Includes an overload scenario (> 100% load) around steps 6-9 so the
    end-to-end pipeline exercises normal and overload regimes.
    """
    recorded_at = start_time + timedelta(seconds=step * step_seconds)

    # Deterministic load profile: baseline 75% + sinusoidal variation + overload pulse
    base_load = 75.0 + 10.0 * math.sin(step * 0.5)
    overload_pulse = 35.0 if 6 <= (step % 12) <= 9 else 0.0
    load_pct = round(base_load + overload_pulse, 2)

    # Deterministic ambient temperature around 28 C
    ambient_temp_c = round(28.0 + 3.0 * math.sin(step * 0.25), 2)

    # Deterministic top-oil temperature responding to ambient and load^2
    thermal_rise = 22.0 * ((load_pct / 100.0) ** 1.6)
    top_oil_temp_c = round(ambient_temp_c + thermal_rise, 2)

    return {
        "event_id": f"{asset_id.lower()}-step-{step:06d}",
        "asset_id": asset_id,
        "recorded_at": recorded_at.isoformat(),
        "load_pct": load_pct,
        "ambient_temp_c": ambient_temp_c,
        "top_oil_temp_c": top_oil_temp_c,
    }


def run_simulator(
    base_url: str,
    asset_id: str,
    steps: int,
    interval_seconds: float,
    start_time: datetime = DEFAULT_START_TIME,
) -> int:
    """Send telemetry readings via HTTP POST. `steps == 0` runs continuously."""
    url = f"{base_url.rstrip('/')}/telemetry"
    sent = 0

    with httpx.Client(timeout=10.0) as client:
        step = 0
        while steps == 0 or step < steps:
            payload = generate_reading(
                step=step,
                asset_id=asset_id,
                start_time=start_time,
            )
            response = client.post(url, json=payload)
            if response.status_code not in (200, 201):
                print(
                    f"[step {step}] ERROR {response.status_code}: {response.text}",
                    file=sys.stderr,
                )
                return 1

            print(
                f"[step {step}] {response.status_code} event_id={payload['event_id']} "
                f"load={payload['load_pct']}% top_oil={payload['top_oil_temp_c']}C"
            )
            sent += 1
            step += 1

            if (steps == 0 or step < steps) and interval_seconds > 0:
                time.sleep(interval_seconds)

    print(f"Completed: sent {sent} reading(s) to {url}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deterministic HTTP telemetry simulator for TX-001."
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Telemetry API base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--asset-id",
        default=DEFAULT_ASSET_ID,
        help=f"Asset ID to simulate (default: {DEFAULT_ASSET_ID})",
    )
    parser.add_argument(
        "--steps",
        type=int,
        default=12,
        help="Number of readings to send; 0 runs continuously (default: 12)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.1,
        help="Seconds to sleep between readings (default: 0.1)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.steps < 0:
        print("--steps must be >= 0", file=sys.stderr)
        return 2
    if args.interval < 0:
        print("--interval must be >= 0", file=sys.stderr)
        return 2
    return run_simulator(
        base_url=args.base_url,
        asset_id=args.asset_id,
        steps=args.steps,
        interval_seconds=args.interval,
    )


if __name__ == "__main__":
    raise SystemExit(main())
