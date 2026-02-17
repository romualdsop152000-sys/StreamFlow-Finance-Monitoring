import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import requests
import pandas as pd

BINANCE_BASE_URL = "https://api.binance.com"
KLINES_ENDPOINT = "/api/v3/klines"

def _utc_today_str() -> str:
    """Retourne la date du jour en UTC au format YYYY-MM-DD."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _ensure_dir(path: Path) -> None:
    """Crée le répertoire s'il n'existe pas."""
    path.mkdir(parents=True, exist_ok=True)

def fetch_klines_1m(symbol: str = "BTCUSDT", limit: int = 1000) -> list:
    """
    Récupère les klines (OHLCV) depuis l'API Binance.
    """
    url = f"{BINANCE_BASE_URL}{KLINES_ENDPOINT}"
    params = {
        "symbol": symbol,
        "interval": "1m",
        "limit": limit
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch klines: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return []

def normalize_klines(raw_klines: list, symbol: str = "BTCUSDT") -> list:
    """
    Normalise les klines brutes en dictionnaires structurés.
    """
    if not raw_klines:
        return []
    
    records = []
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    for k in raw_klines:
        record = {
            "open_time": k[0],
            "open": k[1],
            "high": k[2],
            "low": k[3],
            "close": k[4],
            "volume": k[5],
            "close_time": k[6],
            "quote_volume": k[7],
            "trades": k[8],
            "taker_buy_base": k[9],
            "taker_buy_quote": k[10],
            "ignore": k[11],
            "symbol": symbol,
            "interval": "1m",
            "ingested_at_utc": ingested_at,
            "ts_minute_utc": datetime.utcfromtimestamp(k[0] / 1000).strftime("%Y-%m-%dT%H:%M:00Z")
        }
        records.append(record)
    
    return records

def write_raw(records: list, base_dir: str = "data/raw/finance/crypto/binance/btc_usdt", dt: str = None) -> str:
    """
    Écrit les données brutes en JSON partitionné par date.
    """
    if not records:
        print("[WARNING] No records to write")
        return ""
    
    if dt is None:
        dt = _utc_today_str()
    output_dir = Path(base_dir) / f"dt={dt}"
    _ensure_dir(output_dir)
    
    timestamp = datetime.now(timezone.utc).strftime("%H%M%S")
    output_file = output_dir / f"btc_usdt_{timestamp}.json"
    
    with open(output_file, "w") as f:
        json.dump(records, f, indent=2)
    
    print(f"[OK] Wrote {len(records)} records to {output_file}")
    return str(output_file)

def run(symbol: str = "BTCUSDT", limit: int = 5, dt: str = None):
    """
    Point d'entrée principal pour l'ingestion Binance.
    
    Args:
        symbol: Trading pair symbol
        limit: Number of klines to fetch
        dt: Target date partition (YYYY-MM-DD). Uses UTC today if None.
    """
    print(f"[INFO] Fetching {limit} klines for {symbol} (dt={dt or 'today'})")
    
    raw_klines = fetch_klines_1m(symbol=symbol, limit=limit)
    
    if not raw_klines:
        print("[ERROR] No data fetched")
        return
    
    print(f"[INFO] Fetched {len(raw_klines)} klines")
    
    records = normalize_klines(raw_klines, symbol=symbol)
    
    path = write_raw(records, dt=dt)
    print(f"[SUCCESS] Ingestion complete: {path}")

if __name__ == "__main__":
    run()