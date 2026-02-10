import pytest
import os
from pathlib import Path
from datetime import datetime, timezone


class TestDateFunctions:
    """Tests pour les fonctions utilitaires de date."""

    def test_utc_today_str_format(self):
        """Test format de date YYYY-MM-DD."""
        from src.utils.date_func import _utc_today_str
        
        result = _utc_today_str()
        
        # Vérifie le format
        assert len(result) == 10
        assert result[4] == "-"
        assert result[7] == "-"
        
        # Vérifie que c'est une date valide
        datetime.strptime(result, "%Y-%m-%d")

    def test_utc_now_str_format(self):
        """Test format timestamp ISO."""
        from src.utils.date_func import _utc_now_str
        
        result = _utc_now_str()
        
        # Vérifie le format ISO
        assert "T" in result
        assert result.endswith("Z")
        
        # Vérifie que c'est un timestamp valide
        datetime.strptime(result, "%Y-%m-%dT%H:%M:%SZ")

    def test_ensure_dir_creates_directory(self, temp_data_dir):
        """Test création de répertoire."""
        from src.utils.date_func import _ensure_dir
        
        new_path = temp_data_dir / "new" / "nested" / "directory"
        
        assert not new_path.exists()
        
        _ensure_dir(new_path)
        
        assert new_path.exists()
        assert new_path.is_dir()

    def test_ensure_dir_existing_directory(self, temp_data_dir):
        """Test avec répertoire existant (ne doit pas échouer)."""
        from src.utils.date_func import _ensure_dir
        
        # Le répertoire existe déjà
        assert temp_data_dir.exists()
        
        # Ne doit pas lever d'exception
        _ensure_dir(temp_data_dir)
        
        assert temp_data_dir.exists()