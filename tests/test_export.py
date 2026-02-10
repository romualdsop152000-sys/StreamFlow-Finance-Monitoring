import pytest
import os
from unittest.mock import patch, MagicMock


class TestPostgresConfig:
    """Tests pour la configuration PostgreSQL."""

    def test_get_postgres_config_defaults(self):
        """Test configuration par défaut."""
        from src.spark_jobs.export.load_to_warehouse import get_postgres_config
        
        # Clear env vars for test
        with patch.dict(os.environ, {}, clear=True):
            config = get_postgres_config()
        
        assert config["host"] == "postgres"
        assert config["port"] == "5432"
        assert config["database"] == "datalake"

    def test_get_postgres_config_from_env(self):
        """Test configuration depuis variables d'environnement."""
        from src.spark_jobs.export.load_to_warehouse import get_postgres_config
        
        env_vars = {
            "POSTGRES_HOST": "custom-host",
            "POSTGRES_PORT": "5433",
            "POSTGRES_DB": "custom_db",
            "POSTGRES_USER": "custom_user",
            "POSTGRES_PASSWORD": "custom_pass"
        }
        
        with patch.dict(os.environ, env_vars):
            config = get_postgres_config()
        
        assert config["host"] == "custom-host"
        assert config["port"] == "5433"
        assert config["database"] == "custom_db"

    def test_get_jdbc_url_format(self):
        """Test format URL JDBC."""
        from src.spark_jobs.export.load_to_warehouse import get_jdbc_url
        
        config = {
            "host": "localhost",
            "port": "5432",
            "database": "testdb"
        }
        
        url = get_jdbc_url(config)
        
        assert url == "jdbc:postgresql://localhost:5432/testdb"


class TestElasticsearchConfig:
    """Tests pour la configuration Elasticsearch."""

    def test_get_elastic_config_defaults(self):
        """Test configuration par défaut."""
        from src.indexing.index_usage_to_elastic import get_elastic_config
        
        with patch.dict(os.environ, {}, clear=True):
            config = get_elastic_config()
        
        assert config["host"] == "elasticsearch"
        assert config["port"] == "9200"

    def test_get_elastic_config_from_env(self):
        """Test configuration depuis variables d'environnement."""
        from src.indexing.index_usage_to_elastic import get_elastic_config
        
        env_vars = {
            "ELASTICSEARCH_HOST": "custom-es",
            "ELASTICSEARCH_PORT": "9201"
        }
        
        with patch.dict(os.environ, env_vars):
            config = get_elastic_config()
        
        assert config["host"] == "custom-es"
        assert config["port"] == "9201"