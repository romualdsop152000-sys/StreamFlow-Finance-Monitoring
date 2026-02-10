import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
import os
import sys

# Ajouter le chemin du projet
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestBinanceIngestion:
    """Tests pour l'ingestion Binance."""

    def test_normalize_klines_valid_data(self, sample_btc_raw_data):
        """Test normalisation des données brutes Binance."""
        from src.ingestion.binance_btc_usdt import normalize_klines
        
        # Simuler les données brutes de l'API Binance (format liste)
        raw_klines = [
            [1704067200000, "42000.00", "42100.00", "41900.00", "42050.00", 
             "100.5", 1704067259999, "4225275.00", 1500, "50.25", "2112637.50", "0"],
            [1704067260000, "42050.00", "42150.00", "42000.00", "42100.00",
             "120.3", 1704067319999, "5064630.00", 1800, "60.15", "2532315.00", "0"]
        ]
        
        result = normalize_klines(raw_klines, symbol="BTCUSDT")
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["symbol"] == "BTCUSDT"
        assert "open" in result[0]
        assert "close" in result[0]
        assert "volume" in result[0]

    def test_normalize_klines_empty_data(self):
        """Test avec données vides."""
        from src.ingestion.binance_btc_usdt import normalize_klines
        
        result = normalize_klines([], symbol="BTCUSDT")
        
        assert isinstance(result, list)
        assert len(result) == 0

    @patch('src.ingestion.binance_btc_usdt.requests.get')
    def test_fetch_klines_success(self, mock_get):
        """Test fetch API Binance avec mock."""
        from src.ingestion.binance_btc_usdt import fetch_klines_1m
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            [1704067200000, "42000.00", "42100.00", "41900.00", "42050.00",
             "100.5", 1704067259999, "4225275.00", 1500, "50.25", "2112637.50", "0"]
        ]
        mock_get.return_value = mock_response
        
        result = fetch_klines_1m(symbol="BTCUSDT", limit=1)
        
        assert result is not None
        assert len(result) == 1

    @patch('src.ingestion.binance_btc_usdt.requests.get')
    def test_fetch_klines_api_error(self, mock_get):
        """Test gestion d'erreur API."""
        from src.ingestion.binance_btc_usdt import fetch_klines_1m
        
        # Simuler une exception
        mock_get.side_effect = Exception("API Error")
        
        # La fonction doit retourner une liste vide, pas lever d'exception
        result = fetch_klines_1m(symbol="BTCUSDT", limit=1)
        
        assert result == []


class TestYahooFinanceIngestion:
    """Tests pour l'ingestion Yahoo Finance."""

    @patch('src.ingestion.yahoo_finance.yf.Ticker')
    def test_fetch_ohlcv_success(self, mock_ticker):
        """Test fetch Yahoo Finance avec mock."""
        from src.ingestion.yahoo_finance import fetch_ohlcv
        
        # Mock des données avec index nommé 'Datetime'
        mock_data = pd.DataFrame({
            'Open': [15000.0, 15010.0],
            'High': [15020.0, 15030.0],
            'Low': [14990.0, 15000.0],
            'Close': [15010.0, 15025.0],
            'Volume': [1000000, 1200000]
        }, index=pd.to_datetime(['2024-01-01 00:00:00+00:00', '2024-01-01 00:01:00+00:00']))
        mock_data.index.name = 'Datetime'
        
        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = mock_data
        mock_ticker.return_value = mock_ticker_instance
        
        result = fetch_ohlcv("NDAQ", lag=2)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "close" in result.columns
        assert "symbol" in result.columns

    @patch('src.ingestion.yahoo_finance.yf.Ticker')
    def test_fetch_ohlcv_empty_response(self, mock_ticker):
        """Test avec réponse vide."""
        from src.ingestion.yahoo_finance import fetch_ohlcv
        
        # Mock retourne un DataFrame vide
        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = pd.DataFrame()
        mock_ticker.return_value = mock_ticker_instance
        
        result = fetch_ohlcv("INVALID", lag=5)
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_save_raw_data_locally(self, sample_nasdaq_raw_data, temp_data_dir):
        """Test sauvegarde locale des données."""
        from src.ingestion.yahoo_finance import save_raw_data_locally
        
        raw_path = str(temp_data_dir / "raw" / "nasdaq")
        
        result = save_raw_data_locally(sample_nasdaq_raw_data, raw_path=raw_path)
        
        # Vérifier que le résultat n'est pas vide
        assert result != ""
        assert os.path.exists(result)

    def test_save_raw_data_empty_df(self, temp_data_dir):
        """Test avec DataFrame vide."""
        from src.ingestion.yahoo_finance import save_raw_data_locally
        
        raw_path = str(temp_data_dir / "raw" / "nasdaq")
        empty_df = pd.DataFrame()
        
        result = save_raw_data_locally(empty_df, raw_path=raw_path)
        
        # Doit retourner une chaîne vide pour un DataFrame vide
        assert result == ""