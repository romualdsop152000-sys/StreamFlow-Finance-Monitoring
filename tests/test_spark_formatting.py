import pytest
import pandas as pd
import os
from pathlib import Path


class TestFormatBinanceSpark:
    """Tests pour le formatting Binance Spark."""

    def test_format_dataframe_columns(self, sample_formatted_btc_df):
        """Test que toutes les colonnes requises sont présentes."""
        required_columns = ["ts_minute_utc", "open", "high", "low", "close", "volume"]
        
        for col in required_columns:
            assert col in sample_formatted_btc_df.columns

    def test_format_dataframe_no_nulls_in_required(self, sample_formatted_btc_df):
        """Test absence de nulls dans colonnes critiques."""
        critical_cols = ["ts_minute_utc", "close", "volume"]
        
        for col in critical_cols:
            assert sample_formatted_btc_df[col].isna().sum() == 0

    def test_format_dataframe_positive_prices(self, sample_formatted_btc_df):
        """Test que les prix sont positifs."""
        price_cols = ["open", "high", "low", "close"]
        
        for col in price_cols:
            assert (sample_formatted_btc_df[col] > 0).all()

    def test_format_dataframe_high_low_consistency(self, sample_formatted_btc_df):
        """Test que high >= low."""
        assert (sample_formatted_btc_df["high"] >= sample_formatted_btc_df["low"]).all()

    def test_format_dataframe_sorted_by_time(self, sample_formatted_btc_df):
        """Test que les données sont triées par temps."""
        timestamps = sample_formatted_btc_df["ts_minute_utc"].tolist()
        assert timestamps == sorted(timestamps)


class TestFormatYahooSpark:
    """Tests pour le formatting Yahoo Finance Spark."""

    def test_nasdaq_data_structure(self, sample_nasdaq_raw_data):
        """Test structure des données NASDAQ."""
        required_cols = ["datetime", "open", "high", "low", "close", "volume", "symbol"]
        
        for col in required_cols:
            assert col in sample_nasdaq_raw_data.columns

    def test_nasdaq_symbol_correct(self, sample_nasdaq_raw_data):
        """Test que le symbole est correct."""
        assert (sample_nasdaq_raw_data["symbol"] == "NDAQ").all()

    def test_nasdaq_volume_positive(self, sample_nasdaq_raw_data):
        """Test que les volumes sont positifs."""
        assert (sample_nasdaq_raw_data["volume"] > 0).all()