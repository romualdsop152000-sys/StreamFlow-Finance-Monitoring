import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import requests

BINANCE_BASE_URL = "https://api.binance.com"
KLINES_ENDPOINT = "/api/v3/klines"

def _utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def fetch_klines_1m(symbol: str = "BTCUSDT", limit: int = 1000):
    """
    Fetch latest 1-minute klines from Binance.
    limit max is typically 1000.
    """
    params = {"symbol": symbol, "interval": "1m", "limit": limit}
    r = requests.get(BINANCE_BASE_URL + KLINES_ENDPOINT, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize_klines(raw_klines):
    """
    Convert Binance kline array format into dict records.
    Binance format per kline:
    [
      open_time, open, high, low, close, volume,
      close_time, quote_asset_volume, trades,
      taker_buy_base, taker_buy_quote, ignore
    ]
    """
    records = []
    for k in raw_klines:
        records.append({
            "open_time_ms": int(k[0]),
            "open_time_utc": datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).isoformat(),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time_ms": int(k[6]),
            "close_time_utc": datetime.fromtimestamp(int(k[6]) / 1000, tz=timezone.utc).isoformat(),
            "quote_asset_volume": float(k[7]),
            "number_of_trades": int(k[8]),
            "taker_buy_base_asset_volume": float(k[9]),
            "taker_buy_quote_asset_volume": float(k[10]),
            "symbol": "BTCUSDT",
            "interval": "1m",
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "binance_api",
            "ts_minute_utc": datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc).replace(second=0, microsecond=0).isoformat(),
        })
    return records

def write_raw(records, base_dir: str = "data/raw/finance/crypto/binance/btc_usdt"):
    dt = _utc_today_str()
    out_dir = Path(base_dir) / f"dt={dt}"
    _ensure_dir(out_dir)

    out_file = out_dir / "btc_usdt_1min.jsonl"

    # Append JSON Lines (safe for incremental ingestion)
    with out_file.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return str(out_file)

def run(symbol="BTCUSDT"):
    # Fetch latest klines (last ~1000 minutes max)
    raw = fetch_klines_1m(symbol=symbol, limit=1000)
    records = normalize_klines(raw)
    path = write_raw(records)
    print(f"[OK] Wrote {len(records)} records to {path}")

if __name__ == "__main__":
    run()
