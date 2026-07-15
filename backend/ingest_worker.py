"""ThetaData -> cloud database ingestion worker.

Run this on a backend worker/container, not in the browser host. It keeps your
ThetaData credentials private and writes compact aggregate points to Postgres.
"""

from __future__ import annotations

import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import psycopg


FIRST_ORDER = ["delta", "theta", "vega", "rho"]
SECOND_ORDER = ["gamma", "vanna", "charm", "vomma"]
THIRD_ORDER = ["speed", "zomma", "color", "ultima"]
METRICS = FIRST_ORDER + SECOND_ORDER + THIRD_ORDER

DATABASE_URL = os.getenv("DATABASE_URL", "")
SYMBOL = os.getenv("OPTIONS_SYMBOL", "QQQ")
CONTROL_KEY = os.getenv("CONTROL_KEY", "thetadata_ingest")
EXPIRATION = os.getenv("OPTIONS_EXPIRATION", "*")
STRIKE_RANGE = int(os.getenv("STRIKE_RANGE", "12"))
MAX_DTE = int(os.getenv("MAX_DTE", "3"))
UPDATE_SECONDS = float(os.getenv("UPDATE_SECONDS", "5"))
PAUSE_SLEEP_SECONDS = float(os.getenv("PAUSE_SLEEP_SECONDS", str(max(5.0, UPDATE_SECONDS))))
DATAFRAME_TYPE = os.getenv("DATAFRAME_TYPE", "polars")
LOCAL_CSV_ARCHIVE = os.getenv("LOCAL_CSV_ARCHIVE", "false").lower() == "true"
ARCHIVE_DIR = Path(os.getenv("ARCHIVE_DIR", "raw_snapshots"))

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required.")


def load_dotenv() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def theta_client() -> Any:
    from thetadata import ThetaClient

    creds_path = Path("creds.txt")
    if creds_path.exists() and not os.getenv("THETADATA_API_KEY"):
        return ThetaClient(creds_file=str(creds_path), dataframe_type=DATAFRAME_TYPE)
    return ThetaClient(dataframe_type=DATAFRAME_TYPE)


def dataframe_to_records(df: Any) -> List[Dict[str, Any]]:
    if df is None:
        return []
    if hasattr(df, "to_dicts"):
        return df.to_dicts()
    if hasattr(df, "to_dict"):
        return df.to_dict(orient="records")
    return []


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def clean_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{str(key): clean_value(value) for key, value in row.items()} for row in records]


def to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def weighted_atm(records: List[Dict[str, Any]]) -> float:
    prices = [to_float(row.get("underlying_price")) for row in records if to_float(row.get("underlying_price"))]
    if prices:
        return sum(prices) / len(prices)
    strikes = sorted({to_float(row.get("strike")) for row in records if to_float(row.get("strike"))})
    return strikes[len(strikes) // 2] if strikes else 0.0


def summarize(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    underlying = weighted_atm(records)
    strikes = [to_float(row.get("strike")) for row in records if to_float(row.get("strike"))]
    active = records
    if strikes and underlying:
        sorted_strikes = sorted(set(strikes), key=lambda strike: abs(strike - underlying))
        atm_set = set(sorted_strikes[:5])
        active = [row for row in records if to_float(row.get("strike")) in atm_set]
    totals = {metric: sum(to_float(row.get(metric)) for row in active) for metric in METRICS}
    return {
        "timestamp": datetime.now(timezone.utc),
        "symbol": SYMBOL,
        "underlying": underlying,
        "row_count": len(records),
        "totals": totals,
    }


def fetch_snapshot(client: Any) -> List[Dict[str, Any]]:
    df = client.option_snapshot_greeks_all(
        symbol=SYMBOL,
        expiration=EXPIRATION,
        strike="*",
        right="both",
        max_dte=MAX_DTE,
        strike_range=STRIKE_RANGE,
    )
    return clean_records(dataframe_to_records(df))


def save_local_csv(records: List[Dict[str, Any]], timestamp: datetime) -> str:
    ARCHIVE_DIR.mkdir(exist_ok=True)
    path = ARCHIVE_DIR / f"{SYMBOL}_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
    columns = sorted({key for row in records for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)
    return str(path)


def write_point(conn: psycopg.Connection, summary: Dict[str, Any]) -> None:
    totals = summary["totals"]
    conn.execute(
        """
        insert into option_greek_points (
          ts, symbol, expiration_mode, source, underlying, row_count,
          delta, theta, vega, rho, gamma, vanna, charm, vomma, speed, zomma, color, ultima
        )
        values (
          %(ts)s, %(symbol)s, %(expiration_mode)s, 'thetadata_live', %(underlying)s, %(row_count)s,
          %(delta)s, %(theta)s, %(vega)s, %(rho)s, %(gamma)s, %(vanna)s, %(charm)s, %(vomma)s,
          %(speed)s, %(zomma)s, %(color)s, %(ultima)s
        )
        on conflict (symbol, expiration_mode, ts) do update set
          underlying = excluded.underlying,
          row_count = excluded.row_count,
          delta = excluded.delta,
          theta = excluded.theta,
          vega = excluded.vega,
          rho = excluded.rho,
          gamma = excluded.gamma,
          vanna = excluded.vanna,
          charm = excluded.charm,
          vomma = excluded.vomma,
          speed = excluded.speed,
          zomma = excluded.zomma,
          color = excluded.color,
          ultima = excluded.ultima
        """,
        {
            "ts": summary["timestamp"],
            "symbol": summary["symbol"],
            "expiration_mode": "near_dte",
            "underlying": summary["underlying"],
            "row_count": summary["row_count"],
            **{metric: totals.get(metric, 0.0) for metric in METRICS},
        },
    )


def ensure_control_table(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        create table if not exists stream_control (
          control_key text primary key,
          enabled boolean not null default true,
          updated_at timestamptz not null default now()
        )
        """
    )
    conn.execute(
        """
        insert into stream_control (control_key, enabled)
        values (%s, true)
        on conflict (control_key) do nothing
        """,
        (CONTROL_KEY,),
    )
    conn.commit()


def collector_enabled(conn: psycopg.Connection) -> bool:
    cursor = conn.execute(
        "select enabled from stream_control where control_key = %s",
        (CONTROL_KEY,),
    )
    row = cursor.fetchone()
    conn.commit()
    if row is None:
        ensure_control_table(conn)
        return True
    return bool(row[0])


def main() -> None:
    load_dotenv()
    client = None
    with psycopg.connect(DATABASE_URL) as conn:
        ensure_control_table(conn)
        while True:
            start = time.time()
            try:
                if not collector_enabled(conn):
                    print(f"{datetime.now(timezone.utc).isoformat()} collector paused; skipping ThetaData request")
                    time.sleep(PAUSE_SLEEP_SECONDS)
                    continue
                if client is None:
                    client = theta_client()
                records = fetch_snapshot(client)
                summary = summarize(records)
                write_point(conn, summary)
                if LOCAL_CSV_ARCHIVE:
                    csv_path = save_local_csv(records, summary["timestamp"])
                    conn.execute(
                        """
                        insert into raw_snapshot_archives (ts, symbol, row_count, storage_url)
                        values (%s, %s, %s, %s)
                        on conflict (symbol, ts) do nothing
                        """,
                        (summary["timestamp"], SYMBOL, len(records), csv_path),
                    )
                conn.commit()
                print(
                    f"{summary['timestamp'].isoformat()} wrote {SYMBOL} "
                    f"{summary['row_count']} rows gamma={summary['totals']['gamma']:.6f}"
                )
            except Exception as exc:
                conn.rollback()
                print(f"ingest error: {exc}")
            elapsed = time.time() - start
            time.sleep(max(0.5, UPDATE_SECONDS - elapsed))


if __name__ == "__main__":
    main()
