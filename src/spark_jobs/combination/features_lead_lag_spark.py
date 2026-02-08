import argparse
import os
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_args():
    parser = argparse.ArgumentParser(description="Generate lead-lag features between BTC and NASDAQ")
    parser.add_argument("--execution_date", required=True, help="Date d'exécution (YYYY-MM-DD)")
    parser.add_argument("--max_lag_minutes", type=int, default=5, help="Maximum lag in minutes")
    return parser.parse_args()


def main(execution_date: str, max_lag_minutes: int = 5):
    spark = SparkSession.builder \
        .appName("lead_lag_features") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Chemins corrigés
    btc_path = f"data/formatted/finance/crypto/binance/btc_usdt/dt={execution_date}"
    ndaq_path = f"data/formatted/market/yfinance/nasdaq_100/dt={execution_date}"
    output_path = f"data/usage/finance/lead_lag_analysis/dt={execution_date}"

    print(f"[INFO] BTC path: {btc_path}")
    print(f"[INFO] NDAQ path: {ndaq_path}")

    # Vérifier si les fichiers existent
    if not os.path.exists(btc_path):
        raise FileNotFoundError(f"BTC formatted data not found: {btc_path}")
    
    # Lire les données BTC
    btc = spark.read.parquet(btc_path)
    print(f"[INFO] BTC records: {btc.count()}")

    # Vérifier si NASDAQ existe (optionnel pour le moment)
    if os.path.exists(ndaq_path):
        ndaq = spark.read.parquet(ndaq_path)
        print(f"[INFO] NASDAQ records: {ndaq.count()}")
        
        # Renommer les colonnes pour éviter les conflits
        btc_renamed = btc.select(
            F.col("ts_minute_utc").alias("ts_minute_utc"),
            F.col("close").alias("btc_close"),
            F.col("volume").alias("btc_volume"),
            F.col("price_change_pct").alias("btc_change_pct")
        )
        
        ndaq_renamed = ndaq.select(
            F.col("ts_minute_utc").alias("ts_minute_utc"),
            F.col("close").alias("ndaq_close"),
            F.col("volume").alias("ndaq_volume")
        )
        
        # Joindre sur ts_minute_utc
        joined = btc_renamed.join(ndaq_renamed, on="ts_minute_utc", how="inner")
        print(f"[INFO] Joined records: {joined.count()}")
        
    else:
        print(f"[WARN] NASDAQ data not found at {ndaq_path}, using BTC only")
        joined = btc.select(
            F.col("ts_minute_utc"),
            F.col("close").alias("btc_close"),
            F.col("volume").alias("btc_volume"),
            F.col("price_change_pct").alias("btc_change_pct")
        )

    # Créer les features lead-lag
    window_spec = Window.orderBy("ts_minute_utc")
    
    for lag in range(1, max_lag_minutes + 1):
        joined = joined.withColumn(f"btc_close_lag_{lag}", F.lag("btc_close", lag).over(window_spec))
        joined = joined.withColumn(f"btc_close_lead_{lag}", F.lead("btc_close", lag).over(window_spec))

    # Ajouter métadonnées
    joined = joined.withColumn("processed_at_utc", F.current_timestamp())
    joined = joined.withColumn("execution_date", F.lit(execution_date))

    print(f"[INFO] Final schema:")
    joined.printSchema()
    
    print(f"[INFO] Sample data:")
    joined.show(5, truncate=False)

    # Écrire les résultats avec pandas+pyarrow (contourne les problèmes de permissions Hadoop sur WSL)
    os.makedirs(output_path, exist_ok=True)
    
    pdf = joined.toPandas()
    table = pa.Table.from_pandas(pdf)
    parquet_file = os.path.join(output_path, "data.parquet")
    # Utiliser microseconds pour compatibilité avec Spark
    pq.write_table(table, parquet_file, coerce_timestamps='us', allow_truncated_timestamps=True)
    
    # Créer le fichier _SUCCESS
    success_file = os.path.join(output_path, "_SUCCESS")
    with open(success_file, "w") as f:
        pass
    
    print(f"[SUCCESS] Wrote lead-lag features to {output_path}")
    
    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(args.execution_date, args.max_lag_minutes)