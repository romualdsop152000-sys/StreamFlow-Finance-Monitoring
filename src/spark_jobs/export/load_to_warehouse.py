import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Export lead-lag features to PostgreSQL")
    parser.add_argument("--execution_date", required=True, help="Date d'exécution (YYYY-MM-DD)")
    return parser.parse_args()


def get_postgres_config():
    """Configuration PostgreSQL depuis variables d'environnement."""
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "datalake"),
        "user": os.getenv("POSTGRES_USER", "datalake_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "datalake_pass"),
    }


def get_jdbc_url(config: dict) -> str:
    """Construit l'URL JDBC PostgreSQL."""
    return f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"


def main(execution_date: str):
    spark = SparkSession.builder \
        .appName("load_to_warehouse") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Chemin des données usage
    input_path = f"data/usage/finance/lead_lag_analysis/dt={execution_date}"
    
    print(f"[INFO] Reading from: {input_path}")

    # Vérifier si les données existent
    if not os.path.exists(input_path):
        print(f"[ERROR] Usage data not found: {input_path}")
        spark.stop()
        raise FileNotFoundError(f"Usage data not found: {input_path}")

    # Lire les données Parquet
    df = spark.read.parquet(input_path)
    record_count = df.count()
    
    print(f"[INFO] Records to export: {record_count}")
    
    if record_count == 0:
        print("[WARNING] No records to export")
        spark.stop()
        return

    # Ajouter colonnes d'audit
    df = df.withColumn("loaded_at_utc", F.current_timestamp())
    df = df.withColumn("source_file", F.lit(input_path))

    print("[INFO] Schema:")
    df.printSchema()

    # Configuration PostgreSQL
    pg_config = get_postgres_config()
    jdbc_url = get_jdbc_url(pg_config)
    
    print(f"[INFO] Connecting to: {jdbc_url}")

    # Propriétés JDBC
    jdbc_properties = {
        "user": pg_config["user"],
        "password": pg_config["password"],
        "driver": "org.postgresql.Driver",
    }

    # Table cible
    table_name = "lead_lag_features"

    try:
        # Écrire dans PostgreSQL (mode append pour ne pas écraser)
        df.write \
            .jdbc(
                url=jdbc_url,
                table=table_name,
                mode="append",
                properties=jdbc_properties
            )
        
        print(f"[SUCCESS] Exported {record_count} records to {table_name}")
        
    except Exception as e:
        print(f"[ERROR] Failed to export to PostgreSQL: {e}")
        
        # Fallback: sauvegarder en CSV local
        fallback_path = f"data/export/postgres_fallback/dt={execution_date}"
        os.makedirs(fallback_path, exist_ok=True)
        
        pdf = df.toPandas()
        csv_file = os.path.join(fallback_path, "lead_lag_features.csv")
        pdf.to_csv(csv_file, index=False)
        
        print(f"[FALLBACK] Saved to CSV: {csv_file}")

    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(args.execution_date)
