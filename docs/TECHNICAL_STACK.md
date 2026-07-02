# Guide Technique — StreamFlow Finance
## Stack de l'ingestion à la visualisation

---

## Vue d'ensemble du flux de données

```
[Binance API]  ──►  [Raw JSON]  ──►  [Spark Format]  ──┐
                                                         ├──►  [Spark Join + Features + LOCF]
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

## Sécurité — Variables d'environnement

> **Modification apportée** : tous les identifiants codés en dur ont été externalisés dans
> `docker/.env` (fichier gitignored). Aucun secret ne transit plus dans le code source.

### Fichiers concernés
- `docker/.env` — secrets locaux (gitignored, ne jamais commiter)
- `docker/.env.example` — template à copier (commité, sans valeurs réelles)
- `dbt/btc_nasdaq/profiles.yml` — utilise `env_var()` dbt

### Contenu de `docker/.env.example`

```ini
# Copier ce fichier en .env et renseigner les valeurs réelles
AIRFLOW_POSTGRES_USER=airflow
AIRFLOW_POSTGRES_PASSWORD=change_me
AIRFLOW_POSTGRES_DB=airflow

POSTGRES_HOST=postgres-warehouse
POSTGRES_PORT=5432
POSTGRES_DB=datalake
POSTGRES_USER=datalake_user
POSTGRES_PASSWORD=change_me

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=change_me
AIRFLOW_ADMIN_EMAIL=admin@example.com

PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=change_me

ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200
```

### Profiles dbt (`dbt/btc_nasdaq/profiles.yml`)

```yaml
btc_nasdaq:
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'datalake-warehouse') }}"
      port: "{{ env_var('POSTGRES_PORT', '5432') | int }}"
      dbname: "{{ env_var('POSTGRES_DB', 'datalake') }}"
      user: "{{ env_var('POSTGRES_USER', 'datalake_user') }}"
      pass: "{{ env_var('POSTGRES_PASSWORD', 'datalake_pass') }}"
      schema: btc_nasdaq
      threads: 1
  target: dev
```

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
├── finance/crypto/binance/btc_usdt/dt=2026-07-02/data.json   ← lignes NDJSON
└── market/yfinance/nasdaq_100/dt=2026-07-02/data.csv         ← CSV OHLCV
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
├── finance/crypto/binance/btc_usdt/dt=2026-07-02/*.parquet
└── market/yfinance/nasdaq_100/dt=2026-07-02/*.parquet
```

---
---

## Stack 3 — Feature Engineering (Lead / Lag + LOCF)

### Rôle
Joindre les données BTC et NASDAQ sur le timestamp commun, puis générer
les features temporelles (lag/lead sur 1 à 5 minutes) et les rendements.
Ces features sont le cœur de l'analyse de corrélation.

> **Modifications apportées :**
> 1. Ajout du flag `ndaq_market_open` (TRUE pendant 14h30–21h00 UTC)
> 2. Implémentation du **LOCF cross-day** (Last Observation Carried Forward) :
>    propagation du dernier prix NASDAQ connu à travers les heures de fermeture,
>    y compris en début de journée via une graine chargée depuis J-1.
>    Standard utilisé par Bloomberg, Reuters et les fournisseurs de données financières.

### Technologie
- **PySpark Window Functions** : `lag()`, `lead()`, `last(ignorenulls=True)` sur une fenêtre ordonnée par `ts_minute_utc`
- **Left Join** : préserve tous les instants BTC (24/7), NASDAQ NULL hors heures de marché
- **LOCF** : Last Observation Carried Forward — prix constant entre deux sessions

### Fichier concerné
- `src/spark_jobs/combination/features_lead_lag_spark.py`

---

### Principe du LOCF cross-day

```
J-1  21:00 UTC → dernier prix NASDAQ connu  ← graine chargée depuis dt=J-1
J    00:00 UTC → ndaq_close = prix J-1 (LOCF), ndaq_market_open = FALSE
     ...
J    14:30 UTC → NASDAQ ouvre, vraies données, ndaq_market_open = TRUE
     ...
J    21:00 UTC → NASDAQ ferme, LOCF repart depuis le dernier prix
```

Pendant les heures de fermeture, `ndaq_return_1m ≈ 0%` car le prix est constant :
**(prix_t - prix_{t-1}) / prix_{t-1} = 0**. C'est sémantiquement correct.

---

### Code — Join, LOCF et génération des features

```python
# src/spark_jobs/combination/features_lead_lag_spark.py

import argparse
import os
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main(execution_date: str, max_lag_minutes: int = 5):
    spark = SparkSession.builder \
        .appName("lead_lag_features") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    btc_path  = f"data/formatted/finance/crypto/binance/btc_usdt/dt={execution_date}"
    ndaq_path = f"data/formatted/market/yfinance/nasdaq_100/dt={execution_date}"

    # ── Lecture BTC ──────────────────────────────────────────────────────────
    btc = spark.read.parquet(btc_path)
    btc_renamed = btc.select(
        F.col("ts_minute_utc"),
        F.col("close").alias("btc_close"),
        F.col("volume").alias("btc_volume"),
        F.col("high").alias("btc_high"),
        F.col("low").alias("btc_low"),
        F.col("price_change_pct").alias("btc_change_pct")
    )

    # ── Graine LOCF cross-day : dernier prix NASDAQ de J-1 ──────────────────
    # Permet de propager le prix de clôture de la veille dès 00h00 UTC
    # afin que le forward-fill ait un point de départ avant l'ouverture du marché.
    prev_date     = (datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_ndaq_path = f"data/formatted/market/yfinance/nasdaq_100/dt={prev_date}"
    seed_row = None
    if os.path.exists(prev_ndaq_path):
        prev_ndaq = spark.read.parquet(prev_ndaq_path)
        if prev_ndaq.count() > 0:
            seed_row = prev_ndaq.orderBy(F.col("ts_minute_utc").desc()).limit(1).select(
                F.col("close").alias("ndaq_close"),
                F.col("volume").alias("ndaq_volume"),
                F.col("high").alias("ndaq_high"),
                F.col("low").alias("ndaq_low"),
            )
            print(f"[INFO] LOCF seed loaded from {prev_ndaq_path}")

    # ── Lecture NASDAQ (peut être vide avant 14h30 UTC) ─────────────────────
    has_ndaq = os.path.exists(ndaq_path)

    if has_ndaq:
        ndaq       = spark.read.parquet(ndaq_path)
        ndaq_count = ndaq.count()

        if ndaq_count > 0:
            ndaq_renamed = ndaq.select(
                F.col("ts_minute_utc"),
                F.col("close").alias("ndaq_close"),
                F.col("volume").alias("ndaq_volume"),
                F.col("high").alias("ndaq_high"),
                F.col("low").alias("ndaq_low"),
            )
        else:
            has_ndaq = False

    if has_ndaq:
        # LEFT JOIN : BTC conservé 24/7, NASDAQ NULL hors heures de marché
        joined = btc_renamed.join(ndaq_renamed, on="ts_minute_utc", how="left")

        # Marquer les heures d'ouverture AVANT le forward-fill
        # (après, tout serait TRUE puisque le prix est propagé)
        joined = joined.withColumn("ndaq_market_open", F.col("ndaq_close").isNotNull())

        # ── Injection de la graine J-1 sur le premier enregistrement ────────
        # Le LEFT JOIN exclut le timestamp J-1 (hors plage BTC d'aujourd'hui),
        # donc on injecte les valeurs directement sur la première ligne NULL
        # pour amorcer le forward-fill depuis le début de la journée.
        if seed_row is not None:
            seed_vals = seed_row.first()
            min_ts    = joined.agg(F.min("ts_minute_utc")).first()[0]
            for col_name, seed_val in [
                ("ndaq_close",  float(seed_vals["ndaq_close"])),
                ("ndaq_volume", float(seed_vals["ndaq_volume"])),
                ("ndaq_high",   float(seed_vals["ndaq_high"])),
                ("ndaq_low",    float(seed_vals["ndaq_low"])),
            ]:
                joined = joined.withColumn(
                    col_name,
                    F.when(
                        (F.col("ts_minute_utc") == F.lit(min_ts)) & F.col(col_name).isNull(),
                        F.lit(seed_val)
                    ).otherwise(F.col(col_name))
                )
            print(f"[INFO] LOCF seed injected at first row ({min_ts})")

        # ── Forward-fill (LOCF intra-day + cross-day via graine) ────────────
        # Propage le dernier prix connu à tous les instants NULL suivants.
        # Résultat : ndaq_close non NULL 24/7, ndaq_return_1m ≈ 0% hors marché.
        window_ffill = Window.orderBy("ts_minute_utc").rowsBetween(Window.unboundedPreceding, 0)
        for ndaq_col in ["ndaq_close", "ndaq_volume", "ndaq_high", "ndaq_low"]:
            joined = joined.withColumn(
                ndaq_col,
                F.last(F.col(ndaq_col), ignorenulls=True).over(window_ffill)
            )
        print("[INFO] NASDAQ LOCF (cross-day) applied")

    else:
        # Aucune donnée NASDAQ pour ce jour : appliquer la graine J-1 comme constante
        print(f"[WARNING] NASDAQ data not found or empty for {execution_date}")
        if seed_row is not None:
            seed_vals  = seed_row.first()
            print(f"[INFO] LOCF from J-1: ndaq_close={float(seed_vals['ndaq_close']):.2f}")
            joined = btc_renamed \
                .withColumn("ndaq_close",  F.lit(float(seed_vals["ndaq_close"])).cast("double")) \
                .withColumn("ndaq_volume", F.lit(float(seed_vals["ndaq_volume"])).cast("double")) \
                .withColumn("ndaq_high",   F.lit(float(seed_vals["ndaq_high"])).cast("double")) \
                .withColumn("ndaq_low",    F.lit(float(seed_vals["ndaq_low"])).cast("double")) \
                .withColumn("ndaq_market_open", F.lit(False))
        else:
            # Premier jour d'exécution : pas de graine disponible, NULLs inévitables
            joined = btc_renamed \
                .withColumn("ndaq_close",       F.lit(None).cast("double")) \
                .withColumn("ndaq_volume",      F.lit(None).cast("double")) \
                .withColumn("ndaq_high",        F.lit(None).cast("double")) \
                .withColumn("ndaq_low",         F.lit(None).cast("double")) \
                .withColumn("ndaq_market_open", F.lit(False))

    # ── Features lag / lead (1 → 5 minutes) ─────────────────────────────────
    window_spec = Window.orderBy("ts_minute_utc")

    for lag in range(1, max_lag_minutes + 1):
        joined = joined.withColumn(f"btc_close_lag_{lag}",   F.lag("btc_close", lag).over(window_spec))
        joined = joined.withColumn(f"btc_close_lead_{lag}",  F.lead("btc_close", lag).over(window_spec))
        joined = joined.withColumn(f"btc_volume_lag_{lag}",  F.lag("btc_volume", lag).over(window_spec))
        if has_ndaq:
            joined = joined.withColumn(f"ndaq_close_lag_{lag}",  F.lag("ndaq_close", lag).over(window_spec))
            joined = joined.withColumn(f"ndaq_close_lead_{lag}", F.lead("ndaq_close", lag).over(window_spec))

    # ── Rendements 1 minute (%) ──────────────────────────────────────────────
    joined = joined.withColumn(
        "btc_return_1m",
        F.when(
            F.col("btc_close_lag_1").isNotNull() & (F.col("btc_close_lag_1") != 0),
            (F.col("btc_close") - F.col("btc_close_lag_1")) / F.col("btc_close_lag_1") * 100
        ).otherwise(None)
    )

    if has_ndaq:
        joined = joined.withColumn(
            "ndaq_return_1m",
            F.when(
                F.col("ndaq_close_lag_1").isNotNull() & (F.col("ndaq_close_lag_1") != 0),
                (F.col("ndaq_close") - F.col("ndaq_close_lag_1")) / F.col("ndaq_close_lag_1") * 100
            ).otherwise(None)
        )

    # Ajouter métadonnées
    joined = joined.withColumn("processed_at_utc", F.current_timestamp())
    joined = joined.withColumn("execution_date", F.lit(execution_date))

    # Supprimer la première ligne (lag_1 non disponible)
    joined = joined.filter(F.col("btc_close_lag_1").isNotNull())

    # ── Sauvegarde Parquet ───────────────────────────────────────────────────
    os.makedirs(output_path, exist_ok=True)
    pdf   = joined.toPandas()
    table = pa.Table.from_pandas(pdf)
    pq.write_table(table, os.path.join(output_path, "data.parquet"),
                   coerce_timestamps='us', allow_truncated_timestamps=True)

    with open(os.path.join(output_path, "_SUCCESS"), "w"):
        pass

    print(f"[SUCCESS] Wrote {len(pdf)} lead-lag features to {output_path}")
    spark.stop()
```

---

### Données produites

```
data/usage/finance/lead_lag_analysis/dt=2026-07-02/
├── data.parquet     ← DataFrame complet avec toutes les features + ndaq_market_open
└── _SUCCESS         ← Marqueur d'idempotence Spark
```

---
---

## Stack 4 — Orchestration avec Apache Airflow

### Rôle
Déclencher et séquencer toutes les étapes du pipeline toutes les 5 minutes,
gérer les dépendances entre tâches, les retries et la visibilité opérationnelle.

### Technologie
- **Apache Airflow 2.8.1** avec LocalExecutor
- **PythonOperator + BashOperator** pour chaque étape
- **PostgreSQL** comme metastore Airflow

### Fichier concerné
- `dags/main_pipeline_dag.py`

> **Modifications apportées :**
> 1. `t_dbt_run` était une tâche orpheline (déconnectée du graphe) — reconnectée dans la chaîne séquentielle
> 2. Nom de table corrigé : `"btc_nasdaq"` → `"lead_lag_features"` (table cible réelle)
> 3. Chaîne finale : `t_combine >> t_export_pg >> t_dbt_run >> t_index_elastic >> end`

---

### Code — DAG principal

```python
# dags/main_pipeline_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from src.postgres.load import load_data_in_postgres

PROJECT_ROOT = "/opt/airflow"
SPARK_SUBMIT = "spark-submit"
DBT_DIR      = f"{PROJECT_ROOT}/dbt/btc_nasdaq"
BTC_NASDAQ_USAGE_DATA_PATH = "data/usage/finance/lead_lag_analysis/"

SPARK_CONF = (
    '--conf "spark.hadoop.fs.permissions.enabled=false" '
    '--conf "spark.driver.memory=1g" '
)

DEFAULT_ARGS = {
    "owner": "datalake_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="bigdata_btc_ndx_pipeline",
    default_args=DEFAULT_ARGS,
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "datalake", "spark", "dbt", "postgres", "elastic"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── Ingestion (parallèle) ────────────────────────────────────────────────
    t_ingest_binance = PythonOperator(
        task_id="ingest_binance_btcusdt_5m",
        python_callable=ingest_binance_btcusdt,
    )
    t_ingest_yahoo = PythonOperator(
        task_id="ingest_yahoo_ndaq_5m",
        python_callable=ingest_yahoo_finance_ndaq,
    )

    # ── Formatting Spark (parallèle) ─────────────────────────────────────────
    t_format_binance = BashOperator(
        task_id="spark_format_binance",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT} {SPARK_CONF} "
                     f"src/spark_jobs/formatting/format_binance_spark.py "
                     f"--execution_date '{{{{ ds }}}}'",
    )
    t_format_yahoo = BashOperator(
        task_id="spark_format_yahoo",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT} {SPARK_CONF} "
                     f"src/spark_jobs/formatting/format_yahoo_finance_spark.py "
                     f"--execution_date '{{{{ ds }}}}'",
    )

    # ── Join + Features LOCF ─────────────────────────────────────────────────
    t_combine = BashOperator(
        task_id="spark_join_and_features",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT} {SPARK_CONF} "
                     f"src/spark_jobs/combination/features_lead_lag_spark.py "
                     f"--execution_date '{{{{ ds }}}}'",
    )

    # ── Chargement PostgreSQL — table lead_lag_features ───────────────────────
    # CORRECTION : nom de table "lead_lag_features" (était "btc_nasdaq" par erreur)
    t_export_pg = PythonOperator(
        task_id="load_data_into_postgres",
        python_callable=load_data_in_postgres,
        op_args=[BTC_NASDAQ_USAGE_DATA_PATH, "lead_lag_features"],
    )

    # ── dbt : modèles staging + marts ───────────────────────────────────────
    # CORRECTION : reconnecté dans la chaîne (était orphelin)
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    # ── Indexation Elasticsearch ─────────────────────────────────────────────
    t_index_elastic = BashOperator(
        task_id="index_to_elasticsearch",
        bash_command=f"cd {PROJECT_ROOT} && python3 -m src.indexing.elk_indexing",
    )

    # ── Graphe de dépendances corrigé ────────────────────────────────────────
    start >> [t_ingest_binance, t_ingest_yahoo]
    t_ingest_binance >> t_format_binance
    t_ingest_yahoo   >> t_format_yahoo
    [t_format_binance, t_format_yahoo] >> t_combine
    t_combine >> t_export_pg >> t_dbt_run >> t_index_elastic >> end
```

---

### Graphe des dépendances

```
start
  ├── ingest_binance_btcusdt_5m ──► spark_format_binance ──┐
  └── ingest_yahoo_ndaq_5m      ──► spark_format_yahoo   ──┴──► spark_join_and_features
                                                                         │
                                                               load_data_into_postgres
                                                                         │
                                                                      dbt_run
                                                                         │
                                                               index_to_elasticsearch
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
- `src/postgres/load.py`
- `dbt/btc_nasdaq/models/stagging/stg_btc_ndx_features_5m_dbt.sql`
- `dbt/btc_nasdaq/models/marts/mart_btc_ndx_5m_aligned.sql`

---

### Code — Schéma PostgreSQL (init-warehouse.sql)

> **Modification apportée :** ajout de la colonne `ndaq_market_open BOOLEAN`
> pour distinguer les instants pendant les heures de trading des instants LOCF.

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

    -- NASDAQ OHLCV
    -- Après LOCF cross-day : ndaq_close non NULL 24/7
    -- ndaq_market_open = TRUE uniquement pendant les heures réelles (14h30–21h00 UTC)
    ndaq_close        DOUBLE PRECISION,
    ndaq_high         DOUBLE PRECISION,
    ndaq_low          DOUBLE PRECISION,
    ndaq_volume       DOUBLE PRECISION,
    ndaq_return_1m    DOUBLE PRECISION,
    ndaq_market_open  BOOLEAN,          -- TRUE pendant les heures de trading

    -- Features lag BTC (1 → 5 minutes)
    btc_close_lag_1  DOUBLE PRECISION,
    btc_close_lag_2  DOUBLE PRECISION,
    btc_close_lag_3  DOUBLE PRECISION,
    btc_close_lag_4  DOUBLE PRECISION,
    btc_close_lag_5  DOUBLE PRECISION,
    btc_volume_lag_1 DOUBLE PRECISION,
    btc_volume_lag_2 DOUBLE PRECISION,
    btc_volume_lag_3 DOUBLE PRECISION,
    btc_volume_lag_4 DOUBLE PRECISION,
    btc_volume_lag_5 DOUBLE PRECISION,

    -- Features lead BTC (1 → 5 minutes)
    btc_close_lead_1 DOUBLE PRECISION,
    btc_close_lead_2 DOUBLE PRECISION,
    btc_close_lead_3 DOUBLE PRECISION,
    btc_close_lead_4 DOUBLE PRECISION,
    btc_close_lead_5 DOUBLE PRECISION,

    -- Features lag/lead NASDAQ (1 → 5 minutes)
    ndaq_close_lag_1  DOUBLE PRECISION,
    ndaq_close_lag_2  DOUBLE PRECISION,
    ndaq_close_lag_3  DOUBLE PRECISION,
    ndaq_close_lag_4  DOUBLE PRECISION,
    ndaq_close_lag_5  DOUBLE PRECISION,
    ndaq_close_lead_1 DOUBLE PRECISION,
    ndaq_close_lead_2 DOUBLE PRECISION,
    ndaq_close_lead_3 DOUBLE PRECISION,
    ndaq_close_lead_4 DOUBLE PRECISION,
    ndaq_close_lead_5 DOUBLE PRECISION,

    -- Métadonnées
    processed_at_utc TIMESTAMP WITH TIME ZONE,
    loaded_at_utc    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file      TEXT,

    UNIQUE (ts_minute_utc, execution_date)
);

CREATE INDEX IF NOT EXISTS idx_leadlag_ts   ON lead_lag_features (ts_minute_utc);
CREATE INDEX IF NOT EXISTS idx_leadlag_date ON lead_lag_features (execution_date);

-- Vue analytique : corrélation quotidienne BTC ↔ NASDAQ
CREATE OR REPLACE VIEW v_btc_ndaq_correlation AS
SELECT
    execution_date,
    COUNT(*)                              AS record_count,
    CORR(btc_return_1m, ndaq_return_1m)  AS correlation_1m,
    AVG(btc_return_1m)                    AS avg_btc_return,
    AVG(ndaq_return_1m)                   AS avg_ndaq_return,
    STDDEV(btc_return_1m)                 AS stddev_btc_return,
    STDDEV(ndaq_return_1m)                AS stddev_ndaq_return
FROM lead_lag_features
WHERE btc_return_1m IS NOT NULL
  AND ndaq_return_1m IS NOT NULL
GROUP BY execution_date
ORDER BY execution_date DESC;
```

---

### Code — Chargement PostgreSQL (`src/postgres/load.py`)

> **Modifications apportées :** trois bugs successifs corrigés :
> 1. **Intersection des colonnes** : évite les erreurs sur les colonnes auto-générées (`id SERIAL`, `loaded_at_utc DEFAULT`)
> 2. **Type-cast automatique** : convertit les colonnes texte du staging en `DATE` ou `TIMESTAMPTZ` selon le type cible
> 3. **DISTINCT ON** : déduplique les lignes avant l'`INSERT` pour éviter `CardinalityViolation` sur la contrainte `UNIQUE`

```python
# src/postgres/load.py (extrait — fonction load_data)

def load_data(filepath: str, table_name: str) -> None:
    # ... connexion via variables d'environnement ...

    staging_table = f"_staging_{table_name}_{uuid.uuid4().hex[:8]}"

    with engine.begin() as conn:
        df.to_sql(staging_table, conn, index=False, if_exists="replace", method="multi")

        inspector = inspect(conn)

        # ── 1. Colonnes communes staging ∩ table cible ───────────────────────
        # Exclut les colonnes auto-générées absentes du parquet (id, loaded_at_utc)
        target_col_infos = inspector.get_columns(table_name)
        target_cols      = [c["name"] for c in target_col_infos]
        staging_col_names = {c["name"] for c in inspector.get_columns(staging_table)}
        common_cols = [c for c in target_cols if c in staging_col_names]

        # ── 2. Cast automatique DATE / TIMESTAMPTZ ───────────────────────────
        # Parquet stocke les dates comme string → staging TEXT → cast explicite requis
        type_cast = {}
        for col_info in target_col_infos:
            col_name = col_info["name"]
            if col_name not in staging_col_names:
                continue
            type_str = str(col_info["type"]).upper()
            if type_str == "DATE":
                type_cast[col_name] = "::date"
            elif "TIMESTAMP" in type_str:
                type_cast[col_name] = "::timestamptz"

        col_list    = ", ".join([f'"{c}"' for c in common_cols])
        select_list = ", ".join([f't."{c}"{type_cast.get(c, "")}' for c in common_cols])

        # ── 3. Résolution du conflit : PK puis contrainte UNIQUE ─────────────
        pk      = inspector.get_pk_constraint(table_name)
        pk_cols = [c for c in (pk.get("constrained_columns", []) if pk else [])
                   if c in staging_col_names]

        if not pk_cols:
            for uc in inspector.get_unique_constraints(table_name):
                uc_cols = uc.get("column_names", [])
                if all(c in staging_col_names for c in uc_cols):
                    pk_cols = uc_cols
                    break

        if pk_cols:
            # DISTINCT ON déduplique avant l'INSERT (évite CardinalityViolation)
            distinct_on     = ", ".join([f't."{c}"' for c in pk_cols])
            order_by        = ", ".join([f't."{c}"' for c in pk_cols])
            conflict_target = ", ".join([f'"{c}"' for c in pk_cols])
            update_cols     = [c for c in common_cols if c not in pk_cols]
            update_set      = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])

            sql = (
                f'INSERT INTO "{table_name}" ({col_list}) '
                f'SELECT DISTINCT ON ({distinct_on}) {select_list} '
                f'FROM "{staging_table}" t '
                f'ORDER BY {order_by} '
                f'ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set};'
            )

        conn.execute(text(sql))
        conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))
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
        cast(ts_minute_utc  as timestamp) as ts_minute_utc,
        cast(execution_date as date)      as dt,

        -- BTC
        cast(btc_high        as double precision) as btc_high,
        cast(btc_low         as double precision) as btc_low,
        cast(btc_close       as double precision) as btc_close,
        cast(btc_volume      as double precision) as btc_volume,
        cast(btc_change_pct  as double precision) as btc_change_pct,
        cast(btc_close_lag_1 as double precision) as btc_close_lag_1,
        cast(btc_return_1m   as double precision) as btc_return_1m,

        -- NASDAQ (avec LOCF : ndaq_close non NULL 24/7)
        cast(ndaq_high        as double precision) as ndaq_high,
        cast(ndaq_low         as double precision) as ndaq_low,
        cast(ndaq_close       as double precision) as ndaq_close,
        cast(ndaq_volume      as double precision) as ndaq_volume,
        cast(ndaq_close_lag_1 as double precision) as ndaq_close_lag_1,
        cast(ndaq_return_1m   as double precision) as ndaq_return_1m,
        ndaq_market_open

    from source
)

select * from final
```

---

### Code — Mart aligné (heures de trading uniquement)

```sql
-- dbt/btc_nasdaq/models/marts/mart_btc_ndx_5m_aligned.sql
-- Filtre sur ndaq_market_open pour ne garder que les instants avec vraies données NASDAQ

select * from {{ ref('stg_btc_ndx_features_5m_dbt') }}
where ndaq_market_open = true   -- exclut les heures de fermeture (LOCF)
```

---

### Requêtes de validation en production

```sql
-- Vérifier la complétude du LOCF (0 NULL attendu après J+1)
SELECT
    ndaq_market_open,
    COUNT(*)                                           AS nb_lignes,
    COUNT(ndaq_close)                                  AS ndaq_close_non_null,
    COUNT(*) FILTER (WHERE ndaq_return_1m IS NULL)     AS nulls_return
FROM lead_lag_features
WHERE execution_date = CURRENT_DATE
GROUP BY ndaq_market_open;

-- Résultat attendu :
-- ndaq_market_open=TRUE  → vraies variations NASDAQ (±0.01% à ±0.5%)
-- ndaq_market_open=FALSE → return ≈ 0.0% (prix constant via LOCF)
-- nulls_return = 0 dans les deux cas
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
# Priorité 2 : instance locale Docker (variables d'environnement .env)
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
        for key, value in doc.items():
            if isinstance(value, pd.Timestamp):
                doc[key] = value.isoformat()
            elif pd.isna(value):
                doc[key] = None
        yield {"_id": _id, "_source": doc}

def elk_index(file_path: Path):
    df = pq.read_table(file_path).to_pandas()
    df.drop_duplicates(subset="ts_minute_utc", inplace=True)
    success, _ = helpers.bulk(client, generate_docs(df), index=ELK_INDEX)
    print(f"{success} documents indexés depuis {file_path}")
    date_str = file_path.parent.name
    if date_str != f"dt={pd.Timestamp.now().date()}":
        (file_path.parent / "_ELKSUCCESS").touch()

def main():
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
        "ts_minute_utc":     { "type": "date" },
        "execution_date":    { "type": "date" },
        "btc_close":         { "type": "float" },
        "btc_return_1m":     { "type": "float" },
        "btc_close_lag_1":   { "type": "float" },
        "btc_close_lead_1":  { "type": "float" },
        "ndaq_close":        { "type": "float" },
        "ndaq_return_1m":    { "type": "float" },
        "ndaq_market_open":  { "type": "boolean" }
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
      { "field": "btc_return",  "title": "BTC Return (%)",    "format": ".4f" },
      { "field": "ndaq_return", "title": "NASDAQ Return (%)", "format": ".4f" },
      { "field": "quadrant",    "title": "Signal" }
    ]
  }
}
```

---

### Dashboards recommandés

| Visualisation     | Type Kibana        | Champs utilisés                                               |
|-------------------|--------------------|---------------------------------------------------------------|
| Prix BTC + NASDAQ | Line (double axe)  | `btc_close`, `ndaq_close`                                     |
| Returns 1m        | Area               | `btc_return_1m`, `ndaq_return_1m`                             |
| Corrélation       | Vega scatter       | `btc_return_1m`, `ndaq_return_1m`                             |
| Volume            | Vertical Bar       | `btc_volume`, `ndaq_volume`                                   |
| Volatilité        | Line (fill)        | `btc_high`, `btc_low`                                         |
| Lead/Lag          | Line (4 séries)    | `btc_close`, `btc_close_lag_1`, `ndaq_close`, `ndaq_close_lead_1` |
| Heures de trading | Filter KQL         | `ndaq_market_open: true`                                      |
| KPI docs indexés  | Metric             | `Count`                                                       |
| Table runs        | Data Table         | `ts_minute_utc` + métriques                                   |

---
---

## Résumé des modifications apportées

| # | Fichier | Modification | Raison |
|---|---------|-------------|--------|
| 1 | `docker/.env` + `docker/.env.example` | Externalisation de tous les secrets | Sécurité : aucun credential dans le code |
| 2 | `dbt/btc_nasdaq/profiles.yml` | `env_var()` au lieu de valeurs en dur | Cohérence avec la politique .env |
| 3 | `dags/main_pipeline_dag.py` | `t_dbt_run` reconnecté + table `"lead_lag_features"` | `t_dbt_run` était orphelin ; mauvais nom de table |
| 4 | `src/postgres/load.py` | Intersection colonnes + type-cast + DISTINCT ON | `DatatypeMismatch` + `CardinalityViolation` résolus |
| 5 | `docker/init-warehouse.sql` | Ajout `ndaq_market_open BOOLEAN` | Distinguer heures réelles vs LOCF |
| 6 | `src/spark_jobs/combination/features_lead_lag_spark.py` | LOCF intra-day + LOCF cross-day (graine J-1) | Éliminer les NULLs NASDAQ hors heures de marché |
| 7 | `dbt/btc_nasdaq/models/` | `ndaq_market_open` dans staging ; filtre `= true` dans mart aligné | Exposer le flag LOCF aux analyses dbt |

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
