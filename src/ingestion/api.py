import requests
import pandas as pd
from datetime import datetime

class MarketAPI:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url

    def get_realtime_price(self, symbol: str, interval="1min", outputsize=100):
        """
        Récupère les données OHLC en quasi temps réel
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": self.api_key,
            "format": "JSON"
        }

        response = requests.get(self.base_url, params=params)
        response.raise_for_status()

        data = response.json()

        if "values" not in data:
            raise ValueError(f"Erreur API : {data}")

        df = pd.DataFrame(data["values"])

        # Conversion types
        df["datetime"] = pd.to_datetime(df["datetime"])
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].astype(float)

        # Ajout métadonnées
        df["symbol"] = symbol
        df["source"] = "twelve_data"
        df["fetched_at"] = datetime.utcnow()

        return df.sort_values("datetime")
