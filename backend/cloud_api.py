"""Cloud API for the hosted ThetaData dashboard.

Deploy this on a backend host such as Render, Fly.io, Railway, AWS, or a
container service. Keep ThetaData credentials and DATABASE_URL on the backend,
never in the hosted browser dashboard.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


METRICS = [
    "delta",
    "theta",
    "vega",
    "rho",
    "gamma",
    "vanna",
    "charm",
    "vomma",
    "speed",
    "zomma",
    "color",
    "ultima",
]

DATABASE_URL = os.getenv("DATABASE_URL", "")
DEFAULT_SYMBOL = os.getenv("OPTIONS_SYMBOL", "QQQ")
CONTROL_KEY = os.getenv("CONTROL_KEY", "thetadata_ingest")
CONTROL_TOKEN = os.getenv("CONTROL_TOKEN", "")
CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "*").split(",")
    if origin.strip()
]
MAX_API_POINTS = int(os.getenv("MAX_API_POINTS", "12000"))

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required.")

pool = AsyncConnectionPool(DATABASE_URL, kwargs={"row_factory": dict_row}, open=False)
app = FastAPI(title="ThetaData Greeks Cloud API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup() -> None:
    await pool.open()
    await ensure_control_table()


@app.on_event("shutdown")
async def shutdown() -> None:
    await pool.close()


def point_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    ts = row["ts"]
    if isinstance(ts, datetime):
        timestamp = ts.astimezone(timezone.utc).isoformat()
        timestamp_ms = int(ts.timestamp() * 1000)
    else:
        parsed = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        timestamp = parsed.astimezone(timezone.utc).isoformat()
        timestamp_ms = int(parsed.timestamp() * 1000)
    return {
        "timestamp": timestamp,
        "timestampMs": timestamp_ms,
        "symbol": row["symbol"],
        "underlying": float(row.get("underlying") or 0),
        "rowCount": int(row.get("row_count") or 0),
        "totals": {metric: float(row.get(metric) or 0) for metric in METRICS},
        "source": row.get("source") or "cloud_db",
    }


def summary_from_point(point: Dict[str, Any]) -> Dict[str, Any]:
    totals = point["totals"]
    return {
        "symbol": point["symbol"],
        "underlying_price": point["underlying"],
        "row_count": point["rowCount"],
        "near_atm_count": point["rowCount"],
        "near_atm_totals": totals,
        "gamma_mode": "positive gamma / pinning bias" if totals["gamma"] > 0 else "negative gamma / expansion bias",
        "charm_mode": "positive charm / upward delta drift" if totals["charm"] > 0 else "negative charm / downward delta drift",
        "vanna_mode": "positive vanna / vol-crush bullish pressure" if totals["vanna"] > 0 else "negative vanna / vol-crush bearish pressure",
        "speed_mode": "gamma sensitivity rising with price" if totals["speed"] > 0 else "gamma sensitivity falling with price",
    }


class ControlUpdate(BaseModel):
    enabled: bool


async def ensure_control_table() -> None:
    async with pool.connection() as conn:
        await conn.execute(
            """
            create table if not exists stream_control (
              control_key text primary key,
              enabled boolean not null default true,
              updated_at timestamptz not null default now()
            )
            """
        )
        await conn.execute(
            """
            insert into stream_control (control_key, enabled)
            values (%s, true)
            on conflict (control_key) do nothing
            """,
            (CONTROL_KEY,),
        )
        await conn.commit()


def require_control_token(x_control_token: str) -> None:
    if CONTROL_TOKEN and x_control_token != CONTROL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing collector control token.")


async def control_state() -> Dict[str, Any]:
    async with pool.connection() as conn:
        cursor = await conn.execute(
            """
            select control_key, enabled, updated_at
            from stream_control
            where control_key = %s
            """,
            (CONTROL_KEY,),
        )
        row = await cursor.fetchone()
    if not row:
        await ensure_control_table()
        return await control_state()
    updated_at = row["updated_at"].astimezone(timezone.utc)
    return {
        "controlKey": row["control_key"],
        "enabled": bool(row["enabled"]),
        "updatedAt": updated_at.isoformat(),
    }


async def set_control_state(enabled: bool) -> Dict[str, Any]:
    async with pool.connection() as conn:
        cursor = await conn.execute(
            """
            insert into stream_control (control_key, enabled, updated_at)
            values (%s, %s, now())
            on conflict (control_key) do update set
              enabled = excluded.enabled,
              updated_at = now()
            returning control_key, enabled, updated_at
            """,
            (CONTROL_KEY, enabled),
        )
        row = await cursor.fetchone()
        await conn.commit()
    updated_at = row["updated_at"].astimezone(timezone.utc)
    return {
        "controlKey": row["control_key"],
        "enabled": bool(row["enabled"]),
        "updatedAt": updated_at.isoformat(),
    }


async def latest_point(symbol: str) -> Dict[str, Any] | None:
    async with pool.connection() as conn:
        row = await conn.execute(
            """
            select *
            from option_greek_points
            where symbol = %s
            order by ts desc
            limit 1
            """,
            (symbol,),
        )
        result = await row.fetchone()
    return point_from_row(result) if result else None


async def symbol_coverage(symbol: str) -> Dict[str, Any]:
    async with pool.connection() as conn:
        cursor = await conn.execute(
            """
            select
              min(ts) as start_ts,
              max(ts) as end_ts,
              count(*)::bigint as point_count
            from option_greek_points
            where symbol = %s
            """,
            (symbol,),
        )
        row = await cursor.fetchone()
    if not row or not row["start_ts"]:
        return {"startMs": None, "endMs": None, "pointCount": 0}
    start_ts = row["start_ts"].astimezone(timezone.utc)
    end_ts = row["end_ts"].astimezone(timezone.utc)
    return {
        "startTimestamp": start_ts.isoformat(),
        "endTimestamp": end_ts.isoformat(),
        "startMs": int(start_ts.timestamp() * 1000),
        "endMs": int(end_ts.timestamp() * 1000),
        "pointCount": int(row["point_count"] or 0),
    }


async def history_points(symbol: str, start_ms: int, end_ms: int, limit: int) -> List[Dict[str, Any]]:
    limit = max(100, min(limit, MAX_API_POINTS))
    bucket_ms = max(1, int((end_ms - start_ms) / limit))
    async with pool.connection() as conn:
        cursor = await conn.execute(
            """
            with bucketed as (
              select
                floor(((extract(epoch from ts) * 1000.0) - %s) / %s)::bigint as bucket,
                *
              from option_greek_points
              where symbol = %s
                and ts >= to_timestamp(%s / 1000.0)
                and ts <= to_timestamp(%s / 1000.0)
            )
            select
              min(ts) as ts,
              min(symbol) as symbol,
              min(expiration_mode) as expiration_mode,
              max(source) as source,
              avg(underlying) as underlying,
              max(row_count)::integer as row_count,
              avg(delta) as delta,
              avg(theta) as theta,
              avg(vega) as vega,
              avg(rho) as rho,
              avg(gamma) as gamma,
              avg(vanna) as vanna,
              avg(charm) as charm,
              avg(vomma) as vomma,
              avg(speed) as speed,
              avg(zomma) as zomma,
              avg(color) as color,
              avg(ultima) as ultima
            from bucketed
            group by bucket
            order by min(ts) asc
            limit %s
            """,
            (start_ms, bucket_ms, symbol, start_ms, end_ms, limit),
        )
        rows = await cursor.fetchall()
    return [point_from_row(row) for row in rows]


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"ok": True, "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/control")
async def get_control() -> Dict[str, Any]:
    state = await control_state()
    return {"ok": True, "collector": state}


@app.post("/api/control")
async def update_control(
    update: ControlUpdate,
    x_control_token: str = Header(default="", alias="X-Control-Token"),
) -> Dict[str, Any]:
    require_control_token(x_control_token)
    state = await set_control_state(update.enabled)
    return {"ok": True, "collector": state}


@app.get("/api/latest")
async def latest(symbol: str = Query(DEFAULT_SYMBOL)) -> Dict[str, Any]:
    point = await latest_point(symbol)
    if not point:
        raise HTTPException(status_code=404, detail=f"No cloud data found for {symbol}. Start ingest_worker.py first.")
    coverage = await symbol_coverage(symbol)
    return {
        "ok": True,
        "timestamp": point["timestamp"],
        "config": {
            "symbol": symbol,
            "expiration": "cloud",
            "update_seconds": int(os.getenv("UPDATE_SECONDS", "5")),
        },
        "summary": summary_from_point(point),
        "point": point,
        "coverage": coverage,
    }


@app.get("/api/history")
async def history(
    symbol: str = Query(DEFAULT_SYMBOL),
    start_ms: int = Query(...),
    end_ms: int = Query(...),
    limit: int = Query(12000),
) -> Dict[str, Any]:
    if end_ms <= start_ms:
        raise HTTPException(status_code=400, detail="end_ms must be greater than start_ms")
    points = await history_points(symbol, start_ms, end_ms, limit)
    coverage = await symbol_coverage(symbol)
    return {
        "ok": True,
        "symbol": symbol,
        "startMs": start_ms,
        "endMs": end_ms,
        "points": points,
        "coverage": coverage,
    }


@app.get("/events")
async def events(symbol: str = Query(DEFAULT_SYMBOL)) -> StreamingResponse:
    async def stream() -> Iterable[str]:
        last_timestamp = ""
        while True:
            try:
                point = await latest_point(symbol)
                if point and point["timestamp"] != last_timestamp:
                    last_timestamp = point["timestamp"]
                    coverage = await symbol_coverage(symbol)
                    payload = {
                        "ok": True,
                        "timestamp": point["timestamp"],
                        "config": {
                            "symbol": symbol,
                            "expiration": "cloud",
                            "update_seconds": int(os.getenv("UPDATE_SECONDS", "5")),
                        },
                        "summary": summary_from_point(point),
                        "point": point,
                        "coverage": coverage,
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                else:
                    yield ": keepalive\n\n"
            except Exception as exc:
                yield f"event: error\ndata: {json.dumps({'ok': False, 'message': str(exc)})}\n\n"
            await asyncio.sleep(float(os.getenv("SSE_POLL_SECONDS", "2")))

    return StreamingResponse(stream(), media_type="text/event-stream")
