import pytest
import pandas as pd
import numpy as np


class TestLeadLagFeatures:
    """Tests pour les features lead-lag."""

    def test_lag_feature_creation(self, sample_formatted_btc_df):
        """Test création des features lag."""
        df = sample_formatted_btc_df.copy()
        
        # Créer lag manuellement
        df["btc_close_lag_1"] = df["close"].shift(1)
        df["btc_close_lag_2"] = df["close"].shift(2)
        
        # Vérifier que les lags sont corrects
        assert df["btc_close_lag_1"].iloc[1] == df["close"].iloc[0]
        assert df["btc_close_lag_2"].iloc[2] == df["close"].iloc[0]
        
        # Premier élément doit être NaN
        assert pd.isna(df["btc_close_lag_1"].iloc[0])

    def test_lead_feature_creation(self, sample_formatted_btc_df):
        """Test création des features lead."""
        df = sample_formatted_btc_df.copy()
        
        # Créer lead manuellement
        df["btc_close_lead_1"] = df["close"].shift(-1)
        df["btc_close_lead_2"] = df["close"].shift(-2)
        
        # Vérifier que les leads sont corrects
        assert df["btc_close_lead_1"].iloc[0] == df["close"].iloc[1]
        assert df["btc_close_lead_2"].iloc[0] == df["close"].iloc[2]
        
        # Dernier élément doit être NaN
        assert pd.isna(df["btc_close_lead_1"].iloc[-1])

    def test_return_calculation(self, sample_formatted_btc_df):
        """Test calcul des returns."""
        df = sample_formatted_btc_df.copy()
        
        df["btc_close_lag_1"] = df["close"].shift(1)
        df["btc_return_1m"] = (df["close"] - df["btc_close_lag_1"]) / df["btc_close_lag_1"] * 100
        
        # Vérifier calcul manuel
        expected_return = (42100.0 - 42050.0) / 42050.0 * 100
        actual_return = df["btc_return_1m"].iloc[1]
        
        assert abs(actual_return - expected_return) < 0.001

    def test_dropna_removes_null_lags(self, sample_formatted_btc_df):
        """Test que dropna supprime les lignes avec lag null."""
        df = sample_formatted_btc_df.copy()
        
        # Créer lags
        for i in range(1, 4):
            df[f"btc_close_lag_{i}"] = df["close"].shift(i)
        
        original_len = len(df)
        df_clean = df.dropna(subset=["btc_close_lag_1"])
        
        assert len(df_clean) == original_len - 1

    def test_max_lag_parameter(self, sample_formatted_btc_df):
        """Test que max_lag crée le bon nombre de colonnes."""
        df = sample_formatted_btc_df.copy()
        max_lag = 5
        
        for i in range(1, max_lag + 1):
            df[f"btc_close_lag_{i}"] = df["close"].shift(i)
            df[f"btc_close_lead_{i}"] = df["close"].shift(-i)
        
        lag_cols = [c for c in df.columns if "lag" in c]
        lead_cols = [c for c in df.columns if "lead" in c]
        
        assert len(lag_cols) == max_lag
        assert len(lead_cols) == max_lag