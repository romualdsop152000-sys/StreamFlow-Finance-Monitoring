from pathlib import Path

import yfinance as yf
import pandas as pd
import os

from src.utils.date_func import _ensure_dir, _utc_today_str

def fetch_ohlcv(ticker: str, period: str="1d", interval: str="1m", lag: int=5) -> pd.DataFrame:
    data = yf.Ticker(ticker)
    
    # Get OHLCV (Open, High, Low, Close, Volume) data since last `lag` (`interval`) unit (`interval`)
    last_data = data.history(period=period, interval=interval)
    df = last_data.tail(lag)[['Open', 'High', 'Low', 'Close', 'Volume']]
    df = df.reset_index()  # brings Datetime index as a column

    # Rename columns to lowercase for consistency
    df.columns = df.columns.str.lower()

    # Ensure UTC + minute key (common join key with Binance)
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df["ts_minute_utc"] = df["datetime"].dt.floor("min").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    df["symbol"] = ticker
    df["source"] = "yfinance"
    df["ingested_at_utc"] = pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    df["interval"] = interval
    return df

def save_raw_data_locally(df_new: pd.DataFrame,
                          raw_path: str="data/raw/market/yfinance/nasdaq_100") -> None:
    dt = _utc_today_str()
    final_raw_path = Path(raw_path).joinpath(f"dt={dt}", "nasdaq_ohlcv.csv")
    _ensure_dir(final_raw_path.parent)
    
    # Stockage (append intelligent)
    if os.path.exists(final_raw_path):
        df_old = pd.read_csv(final_raw_path, parse_dates=["datetime"])
        df_old["datetime"] = pd.to_datetime(df_old["datetime"], utc=True)

        df_final = (
            pd.concat([df_old, df_new], ignore_index=True)
            .drop_duplicates(subset=["ts_minute_utc", "symbol"])
            .sort_values("datetime")
        )
    else:
        df_final = df_new.sort_values("datetime")

    df_final.to_csv(final_raw_path, index=False)
    
def run():
    
    # Searching for NASDAQ ticker: "^IXIC" or "NDAQ"
    dat = yf.Ticker("NDAQ")
    # Get OHLCV data since the last x minutes
    x_minutes = 5
    history = dat.history(period="1d", interval="1m")
    print(f"Last {x_minutes} minutes:")
    print(history.tail(x_minutes)[['Open', 'High', 'Low', 'Close', 'Volume']])
    
    ticker = "NDAQ"
    nasdaq_data = fetch_ohlcv(ticker, lag=x_minutes)
    print(f"Saving {ticker} values")
    print(nasdaq_data)
    save_raw_data_locally(nasdaq_data)
    print("Saving successful!")


if __name__ == "__main__":
    run()