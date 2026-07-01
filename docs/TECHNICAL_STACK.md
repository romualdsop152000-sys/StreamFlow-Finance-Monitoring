# Guide Technique — StreamFlow Finance
## Stack de l'ingestion à la visualisation

---

## Vue d'ensemble du flux de données

```
[Binance API]  ──►  [Raw JSON]  ──►  [Spark Format]  ──┐
                                                         ├──►  [Spark Join + Features]
[Yahoo Finance] ──►  [Raw CSV]  ──►  [Spark Format]  ──┘
                                            │
                    ┌───────────────────────┴───────────────────────┐
                    ▼                                               ▼
             [PostgreSQL]  ──►  [dbt]                     [Elasticsearch]
                                                                    │
                                                               [Kibana]
```

Chaque couche est orchestrée par **Apache Airflow** toutes les 5 minutes.

---

---

## Stack 1 — Ingestion des données de marché

### Rôle
Collecter les données OHLCV (Open, High, Low, Close, Volume) en temps réel depuis
deux sources : l'API Binance pour le Bitcoin et Yahoo Finance pour le NASDAQ-100.

### Technologie
- **Binance REST API** : endpoint `/api/v3/klines`, intervalle 1 minute
- **yfinance** : bibliothèque Python wrappant l'API Yahoo Finance

### Fichiers concernés
- `src/ingestion/binance_btc_usdt.py`
- `src/ingestion/yahoo_finance.py`

---

### Code — Ingestion Binance (BTC/USDT)

```python
# src/ingestion/binance_btc_usdt.py

import requests
from datetime import datetime, timezone

BINANCE_URL = "https://api.binance.com/api/v3/klines"

def fetch_klines_1m(symbol: str = "BTCUSDT", limit: int = 10) -> list:
    """
    Appel à l'API Binance pour récupérer les klines (bougies) 1 minute.
    Retourne une liste de tableaux [open_time, open, high, low, close, volume, ...]
    """
    params = {
        "symbol": symbol,
        "interval": "1m",
        "limit": limit
    }
    response = requests.get(BINANCE_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def normalize_klines(raw: list, symbol: str = "BTCUSDT") -> list[dict]:
    """
    Transforme les tableaux Binance bruts en dictionnaires typés.
    Ajoute les métadonnées : symbol, interval, ingestion_timestamp.
    """
    records = []
    for k in raw:
        records.append({
            "open_time_ms":  k[0],
            "open":          float(k[1]),
            "high":          float(k[2]),
            "low":           float(k[3]),
            "close":         float(k[4]),
            "volume":        float(k[5]),
            "close_time_ms": k[6],
            "quote_asset_volume": float(k[7]),
            "number_of_trades":   int(k[8]),
            "symbol":        symbol,
            "interval":      "1m",
            "ingested_at":   datetime.now(timezone.utc).isoformat()
        })
    return records

def write_raw(records: list, execution_date: str) -> str:
    """
    Persiste les données brutes en JSON partitionné par date.
    Chemin : data/raw/finance/crypto/binance/btc_usdt/dt=YYYY-MM-DD/
    Idempotent : ajoute aux fichiers existants sans doublon.
    """
    import json, os
    from pathlib import Path

    out_dir = Path(f"data/raw/finance/crypto/binance/btc_usdt/dt={execution_date}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.json"

    with open(out_file, "a") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    return str(out_file)
```

---

### Code — Ingestion Yahoo Finance (NASDAQ-100)

```python
# src/ingestion/yahoo_finance.py

import yfinance as yf
import pandas as pd
from pathlib import Path

def fetch_ohlcv(
    ticker: str = "^NDX",
    period: str = "1d",
    interval: str = "1m"
) -> pd.DataFrame:
    """
    Télécharge les données OHLCV du NASDAQ-100 via yfinance.
    Disponible uniquement pendant les heures de marché (14h30–21h00 UTC).
    """
    data = yf.download(
        tickers=ticker,
        period=period,
        interval=interval,
        progress=False,
        auto_adjust=True
    )
    data.reset_index(inplace=True)
    data["symbol"] = ticker
    data["interval"] = interval
    return data

def save_raw_data_locally(df: pd.DataFrame, execution_date: str) -> str:
    """
    Sauvegarde avec déduplication : fusionne avec les données existantes
    si le fichier est déjà présent pour la date du jour.
    """
    out_dir = Path(f"data/raw/market/yfinance/nasdaq_100/dt={execution_date}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.csv"

    if out_file.exists():
        existing = pd.read_csv(out_file)
        df = pd.concat([existing, df]).drop_duplicates(
            subset=["Datetime"], keep="last"
        ).sort_values("Datetime")

    df.to_csv(out_file, index=False)
    return str(out_file)
```

---

### Données produites

```
data/raw/
├── finance/crypto/binance/btc_usdt/dt=2026-06-30/data.json   ← lignes NDJSON
└── market/yfinance/nasdaq_100/dt=2026-06-30/data.csv         ← CSV OHLCV
```

---
---

## Stack 2 — Formatting avec Apache Spark

### Rôle
Nettoyer, typer et enrichir les données brutes pour les rendre exploitables
dans les étapes suivantes. Chaque source est traitée indépendamment en parallèle.

### Technologie
- **Apache Spark 3.5.0** (PySpark) — traitement distribué
- **Parquet** — format de sortie colonnaire, optimisé pour les requêtes analytiques

### Fichiers concernés
- `src/spark_jobs/formatting/format_binance_spark.py`
- `src/spark_jobs/formatting/format_yahoo_finance_spark.py`

---

### Code — Formatting BTC (Binance → Parquet)

```python
# src/spark_jobs/formatting/format_binance_spark.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

def format_binance(execution_date: str):
    spark = SparkSession.builder.appName("format_binance").getOrCreate()

    raw_path = f"data/raw/finance/crypto/binance/btc_usdt/dt={execution_date}"
    out_path = f"data/formatted/finance/crypto/binance/btc_usdt/dt={execution_date}"

    # Lecture des fichiers JSON bruts (NDJSON multi-lignes)
    df = spark.read.json(raw_path)

    # Conversion du timestamp milliseconde → datetime UTC
    df = df.withColumn(
        "ts_minute_utc",
        F.to_timestamp(F.from_unixtime(F.col("open_time_ms") / 1000))
    )

    # Cast des colonnes numériques
    for col_name in ["open", "high", "low", "close", "volume"]:
        df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    # Features calculées
    df = df \
        .withColumn("price_range",      F.col("high") - F.col("low")) \
        .withColumn("price_change",     F.col("close") - F.col("open")) \
        .withColumn("price_change_pct",
                    (F.col("close") - F.col("open")) / F.col("open") * 100) \
        .withColumn("is_bullish",       F.col("close") > F.col("open"))

    # Déduplication et tri
    df = df.dropDuplicates(["ts_minute_utc"]).orderBy("ts_minute_utc")

    # Sauvegarde en Parquet
    df.write.mode("overwrite").parquet(out_path)
    spark.stop()
```

---

### Code — Formatting NASDAQ (Yahoo Finance → Parquet)

```python
# src/spark_jobs/formatting/format_yahoo_finance_spark.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def format_yahoo(execution_date: str):
    spark = SparkSession.builder.appName("format_yahoo").getOrCreate()

    raw_path = f"data/raw/market/yfinance/nasdaq_100/dt={execution_date}"
    out_path = f"data/formatted/market/yfinance/nasdaq_100/dt={execution_date}"

    df = spark.read.option("header", True).csv(raw_path)

    # Normalisation du timestamp (Yahoo renvoie des formats variables)
    df = df.withColumn(
        "ts_minute_utc",
        F.to_timestamp(F.col("Datetime"))
    )

    # Renommage et typage
    df = df \
        .withColumnRenamed("Open",   "open") \
        .withColumnRenamed("High",   "high") \
        .withColumnRenamed("Low",    "low") \
        .withColumnRenamed("Close",  "close") \
        .withColumnRenamed("Volume", "volume")

    for col_name in ["open", "high", "low", "close", "volume"]:
        df = df.withColumn(col_name, F.col(col_name).cast("double"))

    # Mêmes features calculées que pour BTC
    df = df \
        .withColumn("price_change_pct",
                    (F.col("close") - F.col("open")) / F.col("open") * 100) \
        .withColumn("is_bullish", F.col("close") > F.col("open"))

    df = df.dropDuplicates(["ts_minute_utc"]).orderBy("ts_minute_utc")
    df.write.mode("overwrite").parquet(out_path)
    spark.stop()
```

---

### Données produites

```
data/formatted/
├── finance/crypto/binance/btc_usdt/dt=2026-06-30/*.parquet
└── market/yfinance/nasdaq_100/dt=2026-06-30/*.parquet
```

---
---

## Stack 3 — Feature Engineering (Lead / Lag)

### Rôle
Joindre les données BTC et NASDAQ sur le timestamp commun, puis générer
les features temporelles (lag/lead sur 1 à 5 minutes) et les rendements.
Ces features sont le cœur de l'analyse de corrélation.

### Technologie
- **PySpark Window Functions** : `lag()`, `lead()` sur une fenêtre ordonnée par `ts_minute_utc`
- **Left Join** : préserve tous les instants BTC (24/7), NASDAQ NULL hors heures de marché

### Fichier concerné
- `src/spark_jobs/combination/features_lead_lag_spark.py`

---

### Code — Join et génération des features

```python
# src/spark_jobs/combination/features_lead_lag_spark.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Join BTC ◄► NASDAQ sur ts_minute_utc ──────────────────────────────────
# LEFT JOIN : tous les points BTC sont conservés.
# Quand le NASDAQ est fermé → ndaq_close = NULL pour ces instants.
joined = btc_renamed.join(ndaq_renamed, on="ts_minute_utc", how="left")

# ── Fenêtre temporelle (ordre chronologique global) ─────────────────────────
window_spec = Window.orderBy("ts_minute_utc")

# ── Génération des features lag et lead (1 → 5 minutes) ─────────────────────
for lag in range(1, max_lag_minutes + 1):
    # BTC : lag (passé) et lead (futur)
    joined = joined.withColumn(
        f"btc_close_lag_{lag}",
        F.lag("btc_close", lag).over(window_spec)
    )
    joined = joined.withColumn(
        f"btc_close_lead_{lag}",
        F.lead("btc_close", lag).over(window_spec)
    )
    joined = joined.withColumn(
        f"btc_volume_lag_{lag}",
        F.lag("btc_volume", lag).over(window_spec)
    )
    # NASDAQ : même logique (NULL si marché fermé)
    joined = joined.withColumn(
        f"ndaq_close_lag_{lag}",
        F.lag("ndaq_close", lag).over(window_spec)
    )
    joined = joined.withColumn(
        f"ndaq_close_lead_{lag}",
        F.lead("ndaq_close", lag).over(window_spec)
    )

# ── Calcul des rendements 1 minute (%) ──────────────────────────────────────
# Formule : (close_t - close_{t-1}) / close_{t-1} * 100
joined = joined.withColumn(
    "btc_return_1m",
    F.when(
        F.col("btc_close_lag_1").isNotNull() & (F.col("btc_close_lag_1") != 0),
        (F.col("btc_close") - F.col("btc_close_lag_1")) / F.col("btc_close_lag_1") * 100
    ).otherwise(None)
)

joined = joined.withColumn(
    "ndaq_return_1m",
    F.when(
        F.col("ndaq_close_lag_1").isNotNull() & (F.col("ndaq_close_lag_1") != 0),
        (F.col("ndaq_close") - F.col("ndaq_close_lag_1")) / F.col("ndaq_close_lag_1") * 100
    ).otherwise(None)
)

# ── Suppression de la première ligne (pas de lag_1 disponible) ───────────────
joined = joined.filter(F.col("btc_close_lag_1").isNotNull())
```

---

### Données produites

```
data/usage/finance/lead_lag_analysis/dt=2026-06-30/
├── data.parquet     ← DataFrame complet avec toutes les features
└── _SUCCESS         ← Marqueur d'idempotence
```

---
---

## Stack 4 — Orchestration avec Apache Airflow

### Rôle
Déclencher et séquencer toutes les étapes du pipeline toutes les 5 minutes,
gérer les dépendances entre tâches, les retries et la visibilité opérationnelle.

### Technologie
- **Apache Airflow 2.8.1** avec LocalExecutor
- **BashOperator** pour chaque étape (scripts Python + dbt)
- **PostgreSQL** comme metastore Airflow

### Fichier concerné
- `dags/main_pipeline_dag.py`

---

### Code — DAG principal

```python
# dags/main_pipeline_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner":            "datalake_team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="bigdata_btc_ndx_pipeline",
    default_args=default_args,
    schedule_interval="*/5 * * * *",   # toutes les 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── Ingestion (parallèle) ────────────────────────────────────────────────
    ingest_btc = BashOperator(
        task_id="ingest_binance_btcusdt_5m",
        bash_command="cd /opt/airflow && python3 -m src.ingestion.binance_btc_usdt "
                     "--execution_date {{ ds }}"
    )

    ingest_ndaq = BashOperator(
        task_id="ingest_yahoo_ndaq_5m",
        bash_command="cd /opt/airflow && python3 -m src.ingestion.yahoo_finance "
                     "--execution_date {{ ds }}"
    )

    # ── Formatting Spark (parallèle) ─────────────────────────────────────────
    format_btc = BashOperator(
        task_id="spark_format_binance",
        bash_command="cd /opt/airflow && python3 -m src.spark_jobs.formatting.format_binance_spark "
                     "--execution_date {{ ds }}"
    )

    format_ndaq = BashOperator(
        task_id="spark_format_yahoo",
        bash_command="cd /opt/airflow && python3 -m src.spark_jobs.formatting.format_yahoo_finance_spark "
                     "--execution_date {{ ds }}"
    )

    # ── Join + Features ──────────────────────────────────────────────────────
    join_features = BashOperator(
        task_id="spark_join_and_features",
        bash_command="cd /opt/airflow && python3 -m src.spark_jobs.combination.features_lead_lag_spark "
                     "--execution_date {{ ds }}"
    )

    # ── Chargement PostgreSQL ────────────────────────────────────────────────
    load_postgres = BashOperator(
        task_id="load_data_into_postgres",
        bash_command="cd /opt/airflow && python3 -m src.spark_jobs.export.load_to_warehouse "
                     "--execution_date {{ ds }}"
    )

    # ── dbt ─────────────────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/btc_nasdaq && dbt run --profiles-dir ."
    )

    # ── Elasticsearch ────────────────────────────────────────────────────────
    elk_index = BashOperator(
        task_id="index_to_elasticsearch",
        bash_command="cd /opt/airflow && python3 -m src.indexing.elk_indexing"
    )

    # ── Dépendances ──────────────────────────────────────────────────────────
    start >> [ingest_btc, ingest_ndaq]
    ingest_btc  >> format_btc
    ingest_ndaq >> format_ndaq
    [format_btc, format_ndaq] >> join_features
    join_features >> load_postgres
    load_postgres >> [dbt_run, elk_index]
    [dbt_run, elk_index] >> end
```

---

### Graphe des dépendances

```
start
  ├── ingest_binance_btcusdt_5m ──► spark_format_binance ──┐
  └── ingest_yahoo_ndaq_5m      ──► spark_format_yahoo   ──┴──► spark_join_and_features
                                                                         │
                                                               load_data_into_postgres
                                                                    ├── dbt_run
                                                                    └── index_to_elasticsearch
                                                                              │
                                                                             end
```

---
---

## Stack 5 — Data Warehouse PostgreSQL + dbt

### Rôle
Stocker les features dans un schéma relationnel structuré,
puis appliquer des transformations analytiques via dbt pour
produire des modèles prêts à l'emploi (staging + marts).

### Technologie
- **PostgreSQL 13** — stockage persistant
- **dbt-core 1.11.6 + dbt-postgres** — modèles SQL versionés

### Fichiers concernés
- `docker/init-warehouse.sql`
- `dbt/btc_nasdaq/models/stagging/stg_btc_ndx_features_5m_dbt.sql`
- `dbt/btc_nasdaq/models/marts/mart_btc_ndx_5m_aligned.sql`
- `dbt/btc_nasdaq/models/marts/mart_btc_ndx_5m_enriched.sql`

---

### Code — Schéma PostgreSQL (init-warehouse.sql)

```sql
-- Table principale des features lead/lag
CREATE TABLE IF NOT EXISTS lead_lag_features (
    id               SERIAL PRIMARY KEY,
    ts_minute_utc    TIMESTAMP WITH TIME ZONE,
    execution_date   DATE,

    -- BTC OHLCV
    btc_close        DOUBLE PRECISION,
    btc_high         DOUBLE PRECISION,
    btc_low          DOUBLE PRECISION,
    btc_volume       DOUBLE PRECISION,
    btc_change_pct   DOUBLE PRECISION,
    btc_return_1m    DOUBLE PRECISION,

    -- NASDAQ OHLCV (NULL hors heures de marché)
    ndaq_close       DOUBLE PRECISION,
    ndaq_high        DOUBLE PRECISION,
    ndaq_low         DOUBLE PRECISION,
    ndaq_volume      DOUBLE PRECISION,
    ndaq_return_1m   DOUBLE PRECISION,

    -- Features lag BTC (1 → 5 minutes)
    btc_close_lag_1  DOUBLE PRECISION,
    btc_close_lag_2  DOUBLE PRECISION,
    btc_close_lag_3  DOUBLE PRECISION,
    btc_close_lag_4  DOUBLE PRECISION,
    btc_close_lag_5  DOUBLE PRECISION,

    -- Features lead BTC (1 → 5 minutes)
    btc_close_lead_1 DOUBLE PRECISION,
    btc_close_lead_2 DOUBLE PRECISION,
    btc_close_lead_3 DOUBLE PRECISION,
    btc_close_lead_4 DOUBLE PRECISION,
    btc_close_lead_5 DOUBLE PRECISION,

    -- Features lag/lead NASDAQ
    ndaq_close_lag_1  DOUBLE PRECISION,
    ndaq_close_lead_1 DOUBLE PRECISION,

    UNIQUE (ts_minute_utc, execution_date)
);

-- Vue analytique : corrélation quotidienne BTC ↔ NASDAQ
CREATE OR REPLACE VIEW v_btc_ndaq_correlation AS
SELECT
    execution_date,
    COUNT(*)                          AS record_count,
    CORR(btc_return_1m, ndaq_return_1m) AS correlation_1m,
    AVG(btc_return_1m)                AS avg_btc_return,
    AVG(ndaq_return_1m)               AS avg_ndaq_return
FROM lead_lag_features
WHERE btc_return_1m IS NOT NULL
  AND ndaq_return_1m IS NOT NULL
GROUP BY execution_date
ORDER BY execution_date DESC;
```

---

### Code — Modèle dbt Staging

```sql
-- dbt/btc_nasdaq/models/stagging/stg_btc_ndx_features_5m_dbt.sql

with source as (
    select * from {{ source('datalake', 'lead_lag_features') }}
),

final as (
    select
        cast(ts_minute_utc  as timestamp)       as ts_minute_utc,
        cast(execution_date as date)            as dt,

        -- BTC
        cast(btc_high        as double precision) as btc_high,
        cast(btc_low         as double precision) as btc_low,
        cast(btc_close       as double precision) as btc_close,
        cast(btc_volume      as double precision) as btc_volume,
        cast(btc_change_pct  as double precision) as btc_change_pct,
        cast(btc_close_lag_1 as double precision) as btc_close_lag_1,
        cast(btc_return_1m   as double precision) as btc_return_1m,

        -- NASDAQ
        cast(ndaq_high        as double precision) as ndaq_high,
        cast(ndaq_low         as double precision) as ndaq_low,
        cast(ndaq_close       as double precision) as ndaq_close,
        cast(ndaq_volume      as double precision) as ndaq_volume,
        cast(ndaq_close_lag_1 as double precision) as ndaq_close_lag_1,
        cast(ndaq_return_1m   as double precision) as ndaq_return_1m

    from source
)

select * from final
```

---

### Code — Mart aligné (NASDAQ non NULL uniquement)

```sql
-- dbt/btc_nasdaq/models/marts/mart_btc_ndx_5m_aligned.sql

select * from {{ ref('stg_btc_ndx_features_5m_dbt') }}
where ndaq_close is not null   -- uniquement les instants pendant les heures de marché
```

---
---

## Stack 6 — Indexation Elasticsearch

### Rôle
Indexer les données enrichies dans Elasticsearch pour permettre
des recherches full-text, des agrégations et la visualisation temps réel dans Kibana.
Le script est idempotent : un fichier `_ELKSUCCESS` marque les partitions déjà indexées.

### Technologie
- **Elasticsearch 8.11.0** — moteur de recherche et d'analytics
- **elasticsearch-py** — client Python officiel
- **helpers.bulk()** — indexation par lots pour la performance

### Fichier concerné
- `src/indexing/elk_indexing.py`

---

### Code — Indexation Elasticsearch

```python
# src/indexing/elk_indexing.py

import os
from pathlib import Path
from glob import glob
import pandas as pd
import pyarrow.parquet as pq
from elasticsearch import Elasticsearch, helpers

# ── Connexion Elasticsearch ──────────────────────────────────────────────────
# Priorité 1 : endpoint cloud (ELK_ENDPOINT + API key)
# Priorité 2 : instance locale Docker (ELASTICSEARCH_HOST:PORT)
ELK_ENDPOINT = os.getenv("ELK_ENDPOINT")
ES_HOST      = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ES_PORT      = os.getenv("ELASTICSEARCH_PORT", "9200")

if ELK_ENDPOINT:
    client = Elasticsearch(hosts=[ELK_ENDPOINT], api_key=os.getenv("ELK_API_KEY"))
else:
    client = Elasticsearch(hosts=[f"http://{ES_HOST}:{ES_PORT}"])

ELK_INDEX    = "finance"
USAGE_DATA   = "data/usage/finance/lead_lag_analysis/"

def generate_docs(df: pd.DataFrame):
    """
    Générateur de documents Elasticsearch.
    _id = ts_minute_utc (garantit l'idempotence des indexations successives).
    """
    for _, row in df.iterrows():
        doc = row.to_dict()
        _id = doc["ts_minute_utc"].strftime("%Y-%m-%d %H:%M:%S")
        # Conversion des types Python → JSON-compatibles
        for key, value in doc.items():
            if isinstance(value, pd.Timestamp):
                doc[key] = value.isoformat()
            elif pd.isna(value):
                doc[key] = None
        yield {"_id": _id, "_source": doc}

def elk_index(file_path: Path):
    """
    Lit un fichier Parquet et l'indexe en bulk dans Elasticsearch.
    Crée _ELKSUCCESS après indexation réussie (partitions passées uniquement).
    """
    df = pq.read_table(file_path).to_pandas()
    df.drop_duplicates(subset="ts_minute_utc", inplace=True)

    success, _ = helpers.bulk(client, generate_docs(df), index=ELK_INDEX)
    print(f"{success} documents indexés depuis {file_path}")

    # Marqueur d'idempotence (pas créé pour les données du jour)
    date_str = file_path.parent.name  # dt=YYYY-MM-DD
    if date_str != f"dt={pd.Timestamp.now().date()}":
        (file_path.parent / "_ELKSUCCESS").touch()

def main():
    """
    Parcourt toutes les partitions non encore indexées et les envoie à ES.
    """
    folders = glob(USAGE_DATA + "dt=*")
    for folder in folders:
        if not (Path(folder) / "_ELKSUCCESS").exists():
            elk_index(Path(folder) / "data.parquet")
```

---

### Mapping de l'index `finance`

```json
{
  "finance": {
    "mappings": {
      "properties": {
        "ts_minute_utc":    { "type": "date" },
        "execution_date":   { "type": "date" },
        "btc_close":        { "type": "float" },
        "btc_return_1m":    { "type": "float" },
        "btc_close_lag_1":  { "type": "float" },
        "btc_close_lead_1": { "type": "float" },
        "ndaq_close":       { "type": "float" },
        "ndaq_return_1m":   { "type": "float" }
      }
    }
  }
}
```

---
---

## Stack 7 — Visualisation Kibana

### Rôle
Exposer les données financières sous forme de dashboards interactifs :
corrélation BTC/NASDAQ, prix en temps réel, volumes, rendements.

### Technologie
- **Kibana 8.11.0** — interface de visualisation pour Elasticsearch
- **Vega-Lite** — grammaire de visualisation pour les graphiques custom (scatter plot)

### Accès
- URL : http://localhost:5601
- Index pattern configuré : `finance` (champ temporel : `ts_minute_utc`)

---

### Code — Scatter plot de corrélation (Vega-Lite)

```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "Corrélation BTC vs NASDAQ — Returns 1 minute",
  "data": {
    "url": {
      "%context%": true,
      "%timefield%": "ts_minute_utc",
      "index": "finance",
      "body": {
        "size": 1000,
        "_source": ["btc_return_1m", "ndaq_return_1m"]
      }
    },
    "format": { "property": "hits.hits" }
  },
  "transform": [
    { "filter": "isValid(datum._source.btc_return_1m) && isValid(datum._source.ndaq_return_1m)" },
    { "calculate": "datum._source.btc_return_1m",  "as": "btc_return" },
    { "calculate": "datum._source.ndaq_return_1m", "as": "ndaq_return" },
    {
      "calculate": "datum.btc_return > 0 && datum.ndaq_return > 0 ? 'Les deux haussiers' : datum.btc_return < 0 && datum.ndaq_return < 0 ? 'Les deux baissiers' : 'Divergence'",
      "as": "quadrant"
    }
  ],
  "mark": { "type": "point", "filled": true, "size": 60, "opacity": 0.75 },
  "encoding": {
    "x": { "field": "btc_return",  "type": "quantitative", "title": "BTC Return 1m (%)" },
    "y": { "field": "ndaq_return", "type": "quantitative", "title": "NASDAQ Return 1m (%)" },
    "color": {
      "field": "quadrant", "type": "nominal",
      "scale": {
        "domain": ["Les deux haussiers", "Les deux baissiers", "Divergence"],
        "range": ["#00b300", "#cc0000", "#ff9900"]
      }
    },
    "tooltip": [
      { "field": "btc_return",  "title": "BTC Return (%)",   "format": ".4f" },
      { "field": "ndaq_return", "title": "NASDAQ Return (%)", "format": ".4f" },
      { "field": "quadrant",    "title": "Signal" }
    ]
  }
}
```

---

### Dashboards recommandés

| Visualisation | Type Kibana | Champs utilisés |
|---|---|---|
| Prix BTC + NASDAQ | Line (double axe) | `btc_close`, `ndaq_close` |
| Returns 1m | Area | `btc_return_1m`, `ndaq_return_1m` |
| Corrélation | Vega scatter | `btc_return_1m`, `ndaq_return_1m` |
| Volume | Vertical Bar | `btc_volume`, `ndaq_volume` |
| Volatilité | Line (fill) | `btc_high`, `btc_low` |
| Lead/Lag | Line (4 séries) | `btc_close`, `btc_close_lag_1`, `ndaq_close`, `ndaq_close_lead_1` |
| KPI docs indexés | Metric | `Count` |
| Table runs | Data Table | `ts_minute_utc` + métriques |

---

## Génération du PDF

Pour convertir ce guide en PDF :

```bash
# Avec pandoc
pandoc docs/TECHNICAL_STACK.md -o docs/TECHNICAL_STACK.pdf \
  --pdf-engine=xelatex \
  -V geometry:margin=2cm \
  -V fontsize=11pt

# Ou via un navigateur
# Ouvrir le fichier .md dans VS Code → Ctrl+Shift+P → "Markdown: Open Preview"
# Puis Ctrl+P → "Save as PDF"
```

---

*StreamFlow Finance — DATA705 P2 — 2025/2026*
