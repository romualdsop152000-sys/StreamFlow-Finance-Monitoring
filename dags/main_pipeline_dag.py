from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


# -------------------------
# -------------------------
PROJECT_ROOT = "/opt/airflow" 
PYTHON = "python3"
SPARK_SUBMIT = "spark-submit"
DBT_DIR = f"{PROJECT_ROOT}/dbt/btc_leadlag_dbt"

# Config Spark pour éviter les problèmes de chmod sur Windows/WSL
SPARK_CONF = (
    '--conf "spark.hadoop.fs.permissions.enabled=false" '
    '--conf "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs=false" '
)

DEFAULT_ARGS = {
    "owner": "datalake_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# -------------------------
# Wrappers Python (ingestion)
# -------------------------
def ingest_binance_btcusdt(**context):
    """
    Appelle le script d'ingestion Binance (source 1).
    On garde le wrapper pour que ce soit clair/maintenable.
    """
    # Import local (évite des soucis de paths au parse du DAG)
    from src.ingestion.binance_btc_usdt import run
    run(symbol="BTCUSDT")


def ingest_yahoo_finance_ndaq(**context):
    from src.ingestion.yahoo_finance import run
    run()


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="bigdata_btc_ndx_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end Data Lake pipeline: Binance + NDX -> Spark -> Postgres -> dbt -> Elastic",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",  # toutes les 5 minutes (bonus realtime)
    catchup=False,
    max_active_runs=1,
    tags=["bigdata", "datalake", "spark", "dbt", "postgres", "elastic"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1) Ingestion (REST API) — Source 1 (toi)
    t_ingest_binance = PythonOperator(
        task_id="ingest_binance_btcusdt_5m",
        python_callable=ingest_binance_btcusdt,
    )

    t_ingest_yahoo = PythonOperator(
        task_id="ingest_yahoo_ndaq_5m",
        python_callable=ingest_yahoo_finance_ndaq,
    )

    # 2) Formatting (Spark)
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

    # 3) Combination (Spark) — join + features lead-lag
    t_combine = BashOperator(
        task_id="spark_join_and_features",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT} {SPARK_CONF} src/spark_jobs/combination/features_lead_lag_spark.py "
            f"--execution_date '{{{{ ds }}}}'"
        ),
    )

    # 4) Export vers PostgreSQL (Spark JDBC)
    t_export_pg = BashOperator(
        task_id="spark_export_to_postgres",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT} {SPARK_CONF} src/spark_jobs/export/load_to_warehouse.py "
            f"--execution_date '{{{{ ds }}}}'"
        ),
    )

    # 5) dbt : build marts + tests qualité
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run",
    )

    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
    )

    # 6) Indexing Elasticsearch (à implémenter plus tard)
    t_index_elastic = BashOperator(
        task_id="index_to_elasticsearch",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON} -m src.indexing.index_usage_to_elastic --execution_date '{{{{ ds }}}}'"
        ),
    )

    end = EmptyOperator(task_id="end")

    # -------------------------
    # Dépendances
    # -------------------------
    start >> [t_ingest_binance, t_ingest_yahoo] \
        >> t_format_binance >> t_format_yahoo \
        >> t_combine >> t_export_pg \
        >> t_dbt_run >> t_dbt_test \
        >> t_index_elastic >> end