from pathlib import Path
from datetime import datetime, timezone
import os

import yfinance as yf
import pandas as pd

from src.utils.date_func import _ensure_dir, _utc_today_str


def fetch_ohlcv(ticker: str, period: str = "1d", interval: str = "1m", lag: int = 5) -> pd.DataFrame:
    """
    Fetch OHLCV data from Yahoo Finance.
    
    Args:
        ticker: Symbol to fetch (e.g., "NDAQ", "^IXIC")
        period: Time period (e.g., "1d", "5d")
        interval: Data interval (e.g., "1m", "5m")
        lag: Number of recent records to keep
    
    Returns:
        DataFrame with OHLCV data
    """
    try:
        data = yf.Ticker(ticker)
        last_data = data.history(period=period, interval=interval)
        
        # Vérifier si les données sont vides
        if last_data.empty:
            print(f"[WARNING] No data returned for {ticker}")
            return pd.DataFrame()
        
        # Vérifier si les colonnes existent
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in last_data.columns for col in required_cols):
            print(f"[WARNING] Missing columns for {ticker}")
            return pd.DataFrame()
        
        df = last_data.tail(lag)[required_cols].copy()
        df = df.reset_index()

        # Rename columns to lowercase
        df.columns = df.columns.str.lower()

        # Gérer le nom de la colonne d'index (peut être 'datetime' ou 'date')
        if 'datetime' not in df.columns and 'date' in df.columns:
            df = df.rename(columns={'date': 'datetime'})
        elif 'datetime' not in df.columns:
            # Si pas de colonne datetime, utiliser l'index
            df['datetime'] = df.index

        # Ensure UTC + minute key
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        df["ts_minute_utc"] = df["datetime"].dt.floor("min").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        df["symbol"] = ticker
        df["source"] = "yfinance"
        df["ingested_at_utc"] = pd.Timestamp.now(tz='UTC').strftime("%Y-%m-%dT%H:%M:%SZ")
        df["interval"] = interval
        
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch {ticker}: {e}")
        return pd.DataFrame()


def save_raw_data_locally(df_new: pd.DataFrame,
                          raw_path: str = "data/raw/market/yfinance/nasdaq_100") -> str:
    """
    Save raw data to CSV with intelligent append (dedup + sort).
    
    Returns:
        Path to saved file or empty string if no data
    """
    # Vérifier si le DataFrame est vide
    if df_new is None or df_new.empty:
        print("[WARNING] Empty DataFrame, skipping save")
        return ""
    
    # Vérifier si la colonne datetime existe
    if "datetime" not in df_new.columns:
        print("[WARNING] Missing 'datetime' column, skipping save")
        return ""
    
    dt = _utc_today_str()
    final_raw_path = Path(raw_path) / f"dt={dt}" / "nasdaq_ohlcv.csv"
    _ensure_dir(final_raw_path.parent)
    
    # Append intelligent
    if os.path.exists(final_raw_path):
        try:
            df_old = pd.read_csv(final_raw_path, parse_dates=["datetime"])
            df_old["datetime"] = pd.to_datetime(df_old["datetime"], utc=True)
            df_old["ts_minute_utc"] = df_old["ts_minute_utc"].astype(str)
            df_new["ts_minute_utc"] = df_new["ts_minute_utc"].astype(str)

            df_final = (
                pd.concat([df_old, df_new], ignore_index=True)
                .drop_duplicates(subset=["ts_minute_utc", "symbol"])
                .sort_values("datetime")
            )
        except Exception as e:
            print(f"[WARNING] Error reading existing file: {e}")
            df_final = df_new.sort_values("datetime")
    else:
        df_new["ts_minute_utc"] = df_new["ts_minute_utc"].astype(str)
        df_final = df_new.sort_values("datetime")

    df_final.to_csv(final_raw_path, index=False)
    return str(final_raw_path)

    
def run(ticker: str = "NDAQ", lag_minutes: int = 5):
    """
    Main entry point for NASDAQ ingestion.
    """
    print(f"[INFO] Fetching {ticker} data (last {lag_minutes} minutes)")
    
    nasdaq_data = fetch_ohlcv(ticker, lag=lag_minutes)
    
    if nasdaq_data.empty:
        print("[ERROR] No data fetched, aborting")
        return
    
    print(f"[INFO] Fetched {len(nasdaq_data)} records")
    print(nasdaq_data)
    
    path = save_raw_data_locally(nasdaq_data)
    if path:
        print(f"[OK] Saved to {path}")
    else:
        print("[WARNING] Data not saved")


if __name__ == "__main__":
    run()