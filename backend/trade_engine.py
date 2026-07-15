"""20-minute Greek confluence trade alert engine.

This is a deterministic alert model. It does not place trades and it does not
promise that a move will happen; it turns the live aggregate Greeks stream into
a directional, auditable alert when pressure is unusually aligned.
"""

from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from statistics import fmean, pstdev
from typing import Any, Dict, Iterable, List


FIRST_ORDER = ["delta", "theta", "vega", "rho"]
SECOND_ORDER = ["gamma", "vanna", "charm", "vomma"]
THIRD_ORDER = ["speed", "zomma", "color", "ultima"]
METRICS = FIRST_ORDER + SECOND_ORDER + THIRD_ORDER

ENGINE_HORIZON_MINUTES = int(os.getenv("ENGINE_HORIZON_MINUTES", "20"))
ENGINE_LOOKBACK_MINUTES = int(os.getenv("ENGINE_LOOKBACK_MINUTES", "60"))
TARGET_NQ_POINTS = float(os.getenv("TARGET_NQ_POINTS", "50"))
MIN_ALERT_CONFIDENCE = float(os.getenv("MIN_ALERT_CONFIDENCE", "65"))
MIN_ALERT_SCORE = float(os.getenv("MIN_ALERT_SCORE", "0.72"))
MIN_SIGNAL_POINTS = int(os.getenv("MIN_SIGNAL_POINTS", "8"))
NQ_POINT_MULTIPLIER = float(os.getenv("NQ_POINT_MULTIPLIER", "40"))
ALERT_MODEL_VERSION = "greek_confluence_v1"


def clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_timestamp_ms(value: Any) -> int:
    if isinstance(value, datetime):
        return int(value.astimezone(timezone.utc).timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(parsed.astimezone(timezone.utc).timestamp() * 1000)
    return 0


def normalize_point(row: Dict[str, Any]) -> Dict[str, Any]:
    timestamp_value = row.get("timestamp") or row.get("ts")
    timestamp_ms = parse_timestamp_ms(row.get("timestampMs") or timestamp_value)
    if isinstance(timestamp_value, datetime):
        timestamp = timestamp_value.astimezone(timezone.utc).isoformat()
    elif timestamp_value:
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).isoformat()
    else:
        timestamp = datetime.now(timezone.utc).isoformat()

    totals = row.get("totals") or {metric: row.get(metric, 0.0) for metric in METRICS}
    return {
        "timestamp": timestamp,
        "timestampMs": timestamp_ms,
        "symbol": row.get("symbol", ""),
        "underlying": to_float(row.get("underlying") or row.get("underlying_price")),
        "rowCount": int(to_float(row.get("rowCount") or row.get("row_count"))),
        "totals": {metric: to_float(totals.get(metric)) for metric in METRICS},
    }


def normalize_points(points: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = [normalize_point(point) for point in points]
    return sorted((point for point in normalized if point["timestampMs"]), key=lambda point: point["timestampMs"])


def series(points: List[Dict[str, Any]], metric: str) -> List[float]:
    return [to_float(point["totals"].get(metric)) for point in points]


def scaled_latest(points: List[Dict[str, Any]], metric: str) -> float:
    values = series(points, metric)
    if not values:
        return 0.0
    center = fmean(values)
    spread = pstdev(values) if len(values) > 1 else 0.0
    scale = spread or max(abs(center) * 0.35, abs(values[-1]) * 0.35, 1.0)
    return clamp(math.tanh((values[-1] - center) / (scale * 1.6)))


def scaled_slope(points: List[Dict[str, Any]], metric: str) -> float:
    values = series(points, metric)
    if len(values) < 2:
        return 0.0
    spread = pstdev(values) or max(abs(fmean(values)) * 0.25, abs(values[-1]) * 0.25, 1.0)
    return clamp(math.tanh((values[-1] - values[0]) / (spread * 2.4)))


def weighted(parts: Iterable[tuple[float, float]]) -> float:
    total_weight = 0.0
    total = 0.0
    for value, weight in parts:
        total += value * weight
        total_weight += abs(weight)
    return clamp(total / total_weight if total_weight else 0.0)


def agreement(direction: int, values: Iterable[float]) -> float:
    signed = [value for value in values if abs(value) >= 0.08]
    if not signed:
        return 0.0
    aligned = sum(1 for value in signed if math.copysign(1, value) == direction)
    return aligned / len(signed)


def movement_stats(points: List[Dict[str, Any]]) -> Dict[str, float]:
    first, last = points[0], points[-1]
    elapsed_minutes = max((last["timestampMs"] - first["timestampMs"]) / 60000, 1 / 60)
    price_move = last["underlying"] - first["underlying"]
    nq_move = price_move * NQ_POINT_MULTIPLIER
    return {
        "elapsedMinutes": elapsed_minutes,
        "sourceMove": price_move,
        "nqMove": nq_move,
        "nqVelocityPerMinute": nq_move / elapsed_minutes,
        "momentumScore": clamp(math.tanh(nq_move / max(TARGET_NQ_POINTS, 1.0))),
    }


def build_trade_signal(points: Iterable[Dict[str, Any]], symbol: str = "") -> Dict[str, Any]:
    data = normalize_points(points)
    now = datetime.now(timezone.utc).isoformat()
    if not data:
        return {
            "status": "no_data",
            "action": "WAIT",
            "alert": False,
            "model": ALERT_MODEL_VERSION,
            "asOf": now,
            "reason": "No live Greek points are stored yet.",
            "reasons": ["No live Greek points are stored yet."],
        }

    latest = data[-1]
    span_minutes = max((latest["timestampMs"] - data[0]["timestampMs"]) / 60000, 0.0)
    stats = movement_stats(data)
    metric_scores = {metric: scaled_latest(data, metric) for metric in METRICS}
    metric_slopes = {metric: scaled_slope(data, metric) for metric in METRICS}

    delta = metric_scores["delta"]
    theta_decay = -metric_scores["theta"]
    vega = metric_scores["vega"]
    rho = metric_scores["rho"]
    gamma = latest["totals"]["gamma"]
    gamma_score = metric_scores["gamma"]
    charm = metric_scores["charm"]
    vanna = metric_scores["vanna"]
    vomma = metric_scores["vomma"]
    speed = metric_scores["speed"]
    zomma = metric_scores["zomma"]
    color = metric_scores["color"]
    ultima = metric_scores["ultima"]
    momentum = stats["momentumScore"]

    first_score = weighted([
        (delta, 0.40),
        (theta_decay, 0.16),
        (vega, 0.14),
        (rho, 0.08),
        (momentum, 0.22),
    ])

    second_core = weighted([
        (charm, 0.34),
        (vanna, 0.30),
        (delta, 0.14),
        (vomma, 0.10),
        (momentum, 0.12),
    ])
    gamma_multiplier = 1.18 if gamma < 0 else 0.74
    second_score = clamp(second_core * gamma_multiplier + (-gamma_score * 0.10 if gamma < 0 else gamma_score * -0.07))

    third_score = weighted([
        (speed, 0.32),
        (zomma, 0.24),
        (color, 0.24),
        (ultima, 0.20),
    ])

    raw_score = weighted([
        (first_score, 0.28),
        (second_score, 0.44),
        (third_score, 0.16),
        (momentum, 0.12),
    ])
    score = clamp(raw_score * (1.10 if gamma < 0 else 0.88))
    direction = 1 if score > 0 else -1 if score < 0 else 0

    pressure_points = score * TARGET_NQ_POINTS * (0.65 + 0.75 * abs(score))
    trend_points = stats["nqVelocityPerMinute"] * ENGINE_HORIZON_MINUTES
    estimated_nq_points = (pressure_points * 0.58) + (trend_points * 0.42)
    estimated_source_points = estimated_nq_points / max(NQ_POINT_MULTIPLIER, 1e-9)

    readiness = min(1.0, span_minutes / max(ENGINE_HORIZON_MINUTES, 1)) * min(1.0, len(data) / max(MIN_SIGNAL_POINTS, 1))
    align = agreement(direction or 1, [first_score, second_score, third_score, momentum])
    confidence = (28 + 52 * abs(score) + 20 * align) * (0.48 + 0.52 * readiness)
    confidence = round(max(0.0, min(99.0, confidence)), 1)

    action = "WAIT"
    if score >= MIN_ALERT_SCORE:
        action = "LONG"
    elif score <= -MIN_ALERT_SCORE:
        action = "SHORT"

    alert = (
        action != "WAIT"
        and abs(estimated_nq_points) >= TARGET_NQ_POINTS
        and confidence >= MIN_ALERT_CONFIDENCE
        and len(data) >= MIN_SIGNAL_POINTS
    )

    regime = "short gamma expansion" if gamma < 0 else "long gamma pinning"
    reasons = [
        f"{regime}: {'amplifies directional pressure' if gamma < 0 else 'dampens drift and favors mean reversion'}",
        f"charm {'positive/upward delta drift' if latest['totals']['charm'] >= 0 else 'negative/downward delta drift'}",
        f"vanna {'positive/bullish vol-pressure' if latest['totals']['vanna'] >= 0 else 'negative/bearish vol-pressure'}",
        f"first-order score {first_score:+.2f}, second-order score {second_score:+.2f}, third-order score {third_score:+.2f}",
        f"20m estimate {estimated_nq_points:+.1f} NQ pts versus {TARGET_NQ_POINTS:.0f} pt alert threshold",
    ]
    if readiness < 0.95:
        reasons.append(f"warming up: only {span_minutes:.1f} minutes of stored live data in the engine window")

    return {
        "status": "ready" if len(data) >= MIN_SIGNAL_POINTS else "warming_up",
        "action": action,
        "alert": alert,
        "model": ALERT_MODEL_VERSION,
        "asOf": latest["timestamp"],
        "symbol": symbol or latest["symbol"],
        "horizonMinutes": ENGINE_HORIZON_MINUTES,
        "lookbackMinutes": ENGINE_LOOKBACK_MINUTES,
        "targetNqPoints": TARGET_NQ_POINTS,
        "sourceToNqMultiplier": NQ_POINT_MULTIPLIER,
        "score": round(score, 4),
        "confidence": confidence,
        "estimatedNqPoints": round(estimated_nq_points, 2),
        "estimatedSourcePoints": round(estimated_source_points, 4),
        "readiness": round(readiness, 3),
        "regime": regime,
        "orderScores": {
            "first": round(first_score, 4),
            "second": round(second_score, 4),
            "third": round(third_score, 4),
            "momentum": round(momentum, 4),
        },
        "metricScores": {metric: round(metric_scores[metric], 4) for metric in METRICS},
        "metricSlopes": {metric: round(metric_slopes[metric], 4) for metric in METRICS},
        "latestTotals": {metric: round(to_float(latest["totals"][metric]), 6) for metric in METRICS},
        "underlying": round(latest["underlying"], 6),
        "pointsUsed": len(data),
        "spanMinutes": round(span_minutes, 2),
        "reasons": reasons,
    }
