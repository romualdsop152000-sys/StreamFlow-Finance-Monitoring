import pytest
import pandas as pd
import tempfile
import os
from datetime import datetime, timezone
from pathlib import Path


@pytest.fixture
def sample_btc_raw_data():
    """Données brutes BTC simulées."""
    return [
        {
            "open_time": 1704067200000,
            "open": "42000.00",
            "high": "42100.00",
            "low": "41900.00",
            "close": "42050.00",
            "volume": "100.5",
            "close_time": 1704067259999,
            "quote_volume": "4225275.00",
            "trades": 1500,
            "taker_buy_base": "50.25",
            "taker_buy_quote": "2112637.50",
            "ignore": "0",
            "symbol": "BTCUSDT",
            "interval": "1m",
            "ingested_at_utc": "2024-01-01T00:01:00Z"
        },
        {
            "open_time": 1704067260000,
            "open": "42050.00",
            "high": "42150.00",
            "low": "42000.00",
            "close": "42100.00",
            "volume": "120.3",
            "close_time": 1704067319999,
            "quote_volume": "5064630.00",
            "trades": 1800,
            "taker_buy_base": "60.15",
            "taker_buy_quote": "2532315.00",
            "ignore": "0",
            "symbol": "BTCUSDT",
            "interval": "1m",
            "ingested_at_utc": "2024-01-01T00:02:00Z"
        }
    ]


@pytest.fixture
def sample_nasdaq_raw_data():
    """Données brutes NASDAQ simulées."""
    return pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-01 00:00:00+00:00", "2024-01-01 00:01:00+00:00"]),
        "open": [15000.0, 15010.0],
        "high": [15020.0, 15030.0],
        "low": [14990.0, 15000.0],
        "close": [15010.0, 15025.0],
        "volume": [1000000.0, 1200000.0],
        "symbol": ["NDAQ", "NDAQ"],
        "source": ["yfinance", "yfinance"],
        "ts_minute_utc": ["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z"],
        "ingested_at_utc": ["2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z"],
        "interval": ["1m", "1m"]
    })


@pytest.fixture
def sample_formatted_btc_df():
    """DataFrame BTC formaté pour les tests."""
    return pd.DataFrame({
        "ts_minute_utc": pd.to_datetime(["2024-01-01 00:00:00", "2024-01-01 00:01:00", 
                                          "2024-01-01 00:02:00", "2024-01-01 00:03:00",
                                          "2024-01-01 00:04:00", "2024-01-01 00:05:00"]),
        "open": [42000.0, 42050.0, 42100.0, 42080.0, 42120.0, 42150.0],
        "high": [42100.0, 42150.0, 42200.0, 42150.0, 42200.0, 42250.0],
        "low": [41900.0, 42000.0, 42050.0, 42000.0, 42080.0, 42100.0],
        "close": [42050.0, 42100.0, 42080.0, 42120.0, 42150.0, 42200.0],
        "volume": [100.5, 120.3, 115.2, 130.1, 125.5, 140.2],
        "price_change_pct": [0.12, 0.12, -0.05, 0.10, 0.07, 0.12]
    })


@pytest.fixture
def temp_data_dir():
    """Crée un répertoire temporaire pour les tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_execution_date():
    """Date d'exécution pour les tests."""
    return "2024-01-01"