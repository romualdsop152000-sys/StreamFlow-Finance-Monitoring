# StreamFlow Finance

> Pipeline de données financières temps réel — Corrélation BTC / NASDAQ-100

[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE)](https://airflow.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-E25A1C)](https://spark.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.11.6-FF694B)](https://www.getdbt.com/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.11.0-005571)](https://www.elastic.co/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)](https://docs.docker.com/compose/)

---

## Problème métier

Les marchés financiers traditionnels (NASDAQ-100) et les marchés crypto (Bitcoin) évoluent
de plus en plus en corrélation. Les traders et gestionnaires de risque ont besoin de :

- **Détecter en temps réel** les mouvements synchronisés ou divergents entre BTC et NASDAQ
- **Anticiper** les signaux de marché grâce aux features lead/lag (décalages temporels)
- **Monitorer** la qualité et la fraîcheur des données ingérées
- **Historiser** les corrélations pour des analyses quantitatives

StreamFlow Finance répond à ce besoin avec un pipeline automatisé qui ingère, transforme
et expose les données toutes les **5 minutes**, du capteur API jusqu'au tableau de bord Kibana.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCES DE DONNÉES                        │
│   Binance API (BTC/USDT 1m)        Yahoo Finance (NASDAQ-100)   │
└───────────────────────┬─────────────────────┬───────────────────┘
                        │                     │
                        ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    COUCHE RAW  (data/raw/)                       │
│    JSON partitionné dt=YYYY-MM-DD    CSV partitionné dt=...      │
└───────────────────────┬─────────────────────┬───────────────────┘
                        │    Apache Spark      │
                        ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                 COUCHE FORMATTED  (data/formatted/)              │
│              Parquet typé, nettoyé, enrichi (OHLCV)             │
└─────────────────────────────────┬───────────────────────────────┘
                                  │    Spark — Left Join
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                  COUCHE USAGE  (data/usage/)                     │
│         Parquet — Features Lead/Lag 1→5 + Returns 1m            │
└─────────────────┬───────────────────────────┬───────────────────┘
                  │                           │
        ┌─────────▼──────────┐    ┌──────────▼──────────┐
        │   PostgreSQL DWH   │    │    Elasticsearch     │
        │  (datalake:5433)   │    │    (finance index)   │
        └─────────┬──────────┘    └──────────┬──────────┘
                  │                          │
        ┌─────────▼──────────┐    ┌──────────▼──────────┐
        │   dbt (marts)      │    │       Kibana         │
        │  staging + marts   │    │   Dashboards 5601    │
        └────────────────────┘    └─────────────────────┘
                  │
        ┌─────────▼──────────┐
        │      pgAdmin       │
        │   UI SQL  :5050    │
        └────────────────────┘

            Orchestration : Apache Airflow (toutes les 5 min)
```

---

## Stack technique

| Composant | Technologie | Rôle |
|---|---|---|
| Orchestration | Apache Airflow 2.8.1 | Scheduling, dépendances, monitoring |
| Ingestion | Binance REST API + yfinance | Collecte des données de marché |
| Processing | Apache Spark 3.5.0 (PySpark) | Nettoyage, join, feature engineering |
| Data Warehouse | PostgreSQL 13 | Stockage structuré, requêtes SQL |
| Transformation | dbt-core 1.11.6 | Modèles staging et marts analytiques |
| Indexation | Elasticsearch 8.11.0 | Stockage documentaire temps réel |
| Visualisation | Kibana 8.11.0 | Dashboards et monitoring |
| Interface DB | pgAdmin 4 | Exploration SQL du warehouse |
| Infra | Docker Compose | Orchestration des conteneurs |

---

## Prérequis

- Docker Desktop >= 24.0
- WSL2 (Windows) ou Linux/macOS
- 8 Go RAM minimum (Elasticsearch + Spark)
- 10 Go d'espace disque

---

## Lancement du projet

### 1. Cloner le dépôt

```bash
git clone <url-du-repo>
cd StreamFlow-Finance
```

### 2. Configurer les variables d'environnement

```bash
cd docker
cp .env.example .env
```

Éditer `.env` avec les valeurs souhaitées (les valeurs par défaut fonctionnent en local) :

```ini
# Exemple de valeurs à personnaliser en production
POSTGRES_PASSWORD=mon_mot_de_passe_secret
AIRFLOW_ADMIN_PASSWORD=mon_mdp_airflow
PGADMIN_DEFAULT_PASSWORD=mon_mdp_pgadmin
```

> `.env` est dans `.gitignore` — il ne sera jamais commité. Ne jamais mettre de secrets dans le code.

### 3. Construire et démarrer tous les services

```bash
docker compose up --build -d
```

> Le premier build télécharge Spark 3.5.0 et Java 17 (~500 Mo). Compter 5-10 min.

### 4. Vérifier que tous les services sont actifs

```bash
docker compose ps
```

Résultat attendu :

```
NAME                 STATUS
airflow-postgres     Up (healthy)
airflow-webserver    Up (healthy)
airflow-scheduler    Up
datalake-warehouse   Up (healthy)
elasticsearch        Up (healthy)
kibana               Up (healthy)
pgadmin              Up
```

### 5. Activer et déclencher le pipeline

```bash
# Activer le DAG
docker exec airflow-webserver airflow dags unpause bigdata_btc_ndx_pipeline

# Déclencher manuellement un premier run
docker exec airflow-webserver airflow dags trigger bigdata_btc_ndx_pipeline

# Surveiller l'exécution
docker exec airflow-webserver airflow dags list-runs -d bigdata_btc_ndx_pipeline
```

### 6. Arrêter le projet

```bash
# Arrêter sans perdre les données
docker compose down

# Arrêter et supprimer les volumes (reset complet)
docker compose down -v
```

---

## Interfaces disponibles

| Service | URL | Identifiants |
|---|---|---|
| **Airflow UI** | http://localhost:8082 | admin / admin |
| **Kibana** | http://localhost:5601 | — |
| **pgAdmin** | http://localhost:5050 | admin@admin.com / admin |
| **Elasticsearch** | http://localhost:9200 | — |
| **PostgreSQL DWH** | localhost:5433 | datalake_user / datalake_pass |

### Vérification rapide des services

```bash
# Airflow health
curl http://localhost:8082/health

# Elasticsearch cluster
curl http://localhost:9200/_cluster/health?pretty

# Index finance (nombre de documents)
curl http://localhost:9200/finance/_count

# Kibana status
curl http://localhost:5601/api/status
```

---

## Structure du projet

```
StreamFlow-Finance/
├── dags/
│   └── main_pipeline_dag.py          # DAG Airflow principal
├── dbt/btc_nasdaq/
│   ├── models/
│   │   ├── stagging/                 # Modèles staging
│   │   └── marts/                    # Marts analytiques
│   ├── profiles.yml
│   └── dbt_project.yml
├── docker/
│   ├── compose.yml                   # Orchestration Docker
│   ├── .env.example                  # Template de configuration (à copier en .env)
│   ├── .env                          # Secrets locaux (gitignored, ne pas commiter)
│   ├── airflow/Dockerfile            # Image Airflow + Spark + Java
│   ├── airflow/requirements.txt      # Dépendances Airflow
│   └── init-warehouse.sql            # Schéma PostgreSQL initial
├── src/
│   ├── ingestion/
│   │   ├── binance_btc_usdt.py       # Ingestion Binance API
│   │   └── yahoo_finance.py          # Ingestion Yahoo Finance
│   ├── spark_jobs/
│   │   ├── formatting/               # Nettoyage et typage
│   │   ├── combination/              # Join + features lead/lag
│   │   └── export/                   # Export vers PostgreSQL
│   ├── postgres/
│   │   └── load.py                   # Chargement data warehouse
│   ├── indexing/
│   │   └── elk_indexing.py           # Indexation Elasticsearch
│   └── utils/
│       └── date_func.py              # Utilitaires dates/chemins
├── data/
│   ├── raw/                          # Données brutes (JSON/CSV)
│   ├── formatted/                    # Données nettoyées (Parquet)
│   └── usage/                        # Données enrichies (Parquet)
├── tests/                            # Suite de tests pytest
├── pyproject.toml                    # Config projet + dépendances
└── README.md
```

---

## Variables d'environnement

Toutes les variables sont centralisées dans `docker/.env` (copié depuis `docker/.env.example`).
**Aucun secret n'est codé en dur dans le code source.**

| Variable | Valeur par défaut | Description |
|---|---|---|
| `POSTGRES_HOST` | `postgres-warehouse` | Hôte PostgreSQL DWH |
| `POSTGRES_PORT` | `5432` | Port PostgreSQL (interne Docker) |
| `POSTGRES_DB` | `datalake` | Nom de la base |
| `POSTGRES_USER` | `datalake_user` | Utilisateur DWH |
| `POSTGRES_PASSWORD` | — | Mot de passe DWH (à définir dans `.env`) |
| `AIRFLOW_POSTGRES_USER` | `airflow` | Utilisateur PostgreSQL Airflow |
| `AIRFLOW_POSTGRES_PASSWORD` | — | Mot de passe PostgreSQL Airflow |
| `AIRFLOW_ADMIN_USER` | `admin` | Login Airflow UI |
| `AIRFLOW_ADMIN_PASSWORD` | — | Mot de passe Airflow UI |
| `PGADMIN_DEFAULT_EMAIL` | — | Email pgAdmin |
| `PGADMIN_DEFAULT_PASSWORD` | — | Mot de passe pgAdmin |
| `ELASTICSEARCH_HOST` | `elasticsearch` | Hôte Elasticsearch |
| `ELASTICSEARCH_PORT` | `9200` | Port Elasticsearch |

---

## Installation locale (hors Docker)

```bash
# Environnement complet avec tests
pip install -e ".[test]"

# Production uniquement
pip install -e .

# Lancer les tests
pytest

# Lancer les tests avec couverture
pytest --cov=src --cov-report=html
```

> Pour les scripts Python en local, copier `docker/.env` à la racine du projet ou exporter
> les variables manuellement : `export POSTGRES_PASSWORD=...`

---

## Pipeline — Étapes et durées

| Étape | Tâche Airflow | Durée moyenne |
|---|---|---|
| Ingestion BTC | `ingest_binance_btcusdt_5m` | ~5s |
| Ingestion NASDAQ | `ingest_yahoo_ndaq_5m` | ~5s |
| Formatting BTC | `spark_format_binance` | ~40s |
| Formatting NASDAQ | `spark_format_yahoo` | ~40s |
| Join + Features | `spark_join_and_features` | ~25s |
| Chargement PostgreSQL | `load_data_into_postgres` | ~2s |
| Transformation dbt | `dbt_run` | ~10s |
| Indexation Elasticsearch | `index_to_elasticsearch` | ~2s |
| **Total** | | **~2 min 30s** |

---

## Features produites

| Feature | Description |
|---|---|
| `btc_close_lag_1` … `lag_5` | Prix BTC des N minutes précédentes |
| `btc_close_lead_1` … `lead_5` | Prix BTC des N minutes suivantes |
| `btc_volume_lag_1` … `lag_5` | Volume BTC des N minutes précédentes |
| `ndaq_close_lag_1` … `lag_5` | Prix NASDAQ des N minutes précédentes |
| `ndaq_close_lead_1` … `lead_5` | Prix NASDAQ des N minutes suivantes |
| `btc_return_1m` | Rendement BTC sur 1 minute (%) |
| `ndaq_return_1m` | Rendement NASDAQ sur 1 minute (%) |

---

## Cours

DATA705 — Bases de données non relationnelles / Data Lake  
Programme P2 — 2025/2026
