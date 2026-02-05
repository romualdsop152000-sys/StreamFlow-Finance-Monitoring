from pathlib import Path
import yfinance as yf
import pandas as pd
import os

from src.utils.date_func import _ensure_dir, _utc_today_str

# Searching for NASDAQ ticker: "^IXIC" or "NDAQ"
dat = yf.Ticker("NDAQ")
# Get OHLCV data since the last x minutes
x_minutes = 5
history = dat.history(period="1d", interval="1m")
print(f"Last {x_minutes} minutes:")
print(history.tail(x_minutes)[['Open', 'High', 'Low', 'Close', 'Volume']])

def fetch_ohlcv(ticker: str, period: str="1d", interval: str="1m", lag: int=5) -> pd.DataFrame:
    data = yf.Ticker(ticker)
    
    # Get OHLCV (Open, High, Low, Close, Volume) data since last `lag` (`interval`) unit (`interval`)
    last_data = data.history(period=period, interval=interval)
    df = last_data.tail(lag)[['Open', 'High', 'Low', 'Close', 'Volume']]
    df['Symbol'] = ticker
    return df

def save_raw_data_locally(df_new: pd.DataFrame,
                          raw_path: str="data/raw/market/yfinance/nasdaq_100") -> None:
    dt = _utc_today_str()
    final_raw_path = Path(raw_path).joinpath(f"dt={dt}", "nasdaq_ohlcv.csv")
    _ensure_dir(final_raw_path.parent)
    
    # Stockage (append intelligent)
    if os.path.exists(final_raw_path):
        df_old = pd.read_csv(final_raw_path, parse_dates=["Datetime"])
        df_final = (
            pd.concat([df_old, df_new.reset_index()])
            .drop_duplicates(subset=["Datetime", "Symbol"])
            .sort_values("Datetime")
        )
    else:
        df_final = df_new.reset_index()

    df_final.to_csv(final_raw_path, index=False)

if __name__ == "__main__":
    ticker = "NDAQ"
    nasdaq_data = fetch_ohlcv(ticker)
    print("Saving {ticker} values")
    print(nasdaq_data)
    save_raw_data_locally(nasdaq_data)
    print("Saving successful!")
    