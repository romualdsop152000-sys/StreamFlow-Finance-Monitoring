from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


# -------------------------
# Configuration
# -------------------------
PROJECT_ROOT = "/opt/airflow" 
PYTHON = "python3"
SPARK_SUBMIT = "spark-submit"
DBT_DIR = f"{PROJECT_ROOT}/dbt/btc_leadlag_dbt"

# Config Spark pour éviter les problèmes de chmod sur Windows/WSL
SPARK_CONF = (
    '--conf "spark.hadoop.fs.permissions.enabled=false" '
    '--conf "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs=false" '
    '--conf "spark.driver.memory=1g" '
)

DEFAULT_ARGS = {
    "owner": "datalake_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


# -------------------------
# Wrappers Python (ingestion)
# -------------------------
def ingest_binance_btcusdt(**context):
    """Appelle le script d'ingestion Binance."""
    import sys
    sys.path.insert(0, PROJECT_ROOT)
    from src.ingestion.binance_btc_usdt import run
    execution_date = context['ds']  # Format: YYYY-MM-DD
    run(symbol="BTCUSDT", dt=execution_date)


def ingest_yahoo_finance_ndaq(**context):
    """Appelle le script d'ingestion Yahoo Finance."""
    import sys
    sys.path.insert(0, PROJECT_ROOT)
    from src.ingestion.yahoo_finance import run
    execution_date = context['ds']  # Format: YYYY-MM-DD
    run(dt=execution_date)


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="bigdata_btc_ndx_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end Data Lake pipeline: Binance + NDX -> Spark -> Postgres -> dbt -> Elastic",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "datalake", "spark", "dbt", "postgres", "elastic"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ========== 1) INGESTION (parallèle) ==========
    t_ingest_binance = PythonOperator(
        task_id="ingest_binance_btcusdt_5m",
        python_callable=ingest_binance_btcusdt,
    )

    t_ingest_yahoo = PythonOperator(
        task_id="ingest_yahoo_ndaq_5m",
        python_callable=ingest_yahoo_finance_ndaq,
    )

    # ========== 2) FORMATTING (parallèle après ingestion) ==========
    t_format_binance = BashOperator(
        task_id="spark_format_binance",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT} {SPARK_CONF} src/spark_jobs/formatting/format_binance_spark.py "
            f"--execution_date '{{{{ ds }}}}'"
        ),
    )

    t_format_yahoo = BashOperator(
        task_id="spark_format_yahoo",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT} {SPARK_CONF} src/spark_jobs/formatting/format_yahoo_finance_spark.py "
            f"--execution_date '{{{{ ds }}}}'"
        ),
    )

    # ========== 3) COMBINATION (après les deux formats) ==========
    t_combine = BashOperator(
        task_id="spark_join_and_features",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT} {SPARK_CONF} src/spark_jobs/combination/features_lead_lag_spark.py "
            f"--execution_date '{{{{ ds }}}}'"
        ),
    )

    # ========== 4) EXPORT vers PostgreSQL ==========
    t_export_pg = BashOperator(
        task_id="spark_export_to_postgres",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT} {SPARK_CONF} src/spark_jobs/export/load_to_warehouse.py "
            f"--execution_date '{{{{ ds }}}}'"
        ),
    )

    # ========== 5) dbt : build marts + tests qualité ==========
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    # ========== 6) Indexing Elasticsearch ==========
    t_index_elastic = BashOperator(
        task_id="index_to_elasticsearch",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON} -m src.indexing.index_usage_to_elastic --execution_date '{{{{ ds }}}}'"
        ),
    )

    end = EmptyOperator(task_id="end")

    # -------------------------
    # DÉPENDANCES CORRIGÉES
    # -------------------------
    # Ingestion parallèle
    start >> [t_ingest_binance, t_ingest_yahoo]
    
    # Chaque format dépend de son ingestion
    t_ingest_binance >> t_format_binance
    t_ingest_yahoo >> t_format_yahoo
    
    # Combination attend les deux formats
    [t_format_binance, t_format_yahoo] >> t_combine
    
    # Suite séquentielle
    t_combine >> t_export_pg >> t_dbt_run >> t_dbt_test >> t_index_elastic >> end