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
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # CORRECTION: Chemins cohérents
    btc_path = f"data/formatted/finance/crypto/binance/btc_usdt/dt={execution_date}"
    ndaq_path = f"data/formatted/market/yfinance/nasdaq_100/dt={execution_date}"
    output_path = f"data/usage/finance/lead_lag_analysis/dt={execution_date}"

    print(f"[INFO] BTC path: {btc_path}")
    print(f"[INFO] NDAQ path: {ndaq_path}")
    print(f"[INFO] Output path: {output_path}")

    # Vérifier si BTC existe
    if not os.path.exists(btc_path):
        print(f"[ERROR] BTC formatted data not found: {btc_path}")
        spark.stop()
        raise FileNotFoundError(f"BTC formatted data not found: {btc_path}")
    
    # Lire les données BTC
    btc = spark.read.parquet(btc_path)
    btc_count = btc.count()
    print(f"[INFO] BTC records: {btc_count}")

    if btc_count == 0:
        print("[ERROR] BTC DataFrame is empty")
        spark.stop()
        raise ValueError("BTC DataFrame is empty")

    # Colonnes disponibles dans BTC
    btc_cols = btc.columns
    print(f"[INFO] BTC columns: {btc_cols}")

    # Renommer les colonnes BTC
    btc_renamed = btc.select(
        F.col("ts_minute_utc"),
        F.col("close").alias("btc_close"),
        F.col("volume").alias("btc_volume"),
        F.col("high").alias("btc_high"),
        F.col("low").alias("btc_low"),
        F.col("price_change_pct").alias("btc_change_pct") if "price_change_pct" in btc_cols else F.lit(None).cast("double").alias("btc_change_pct")
    )

    # Vérifier si NASDAQ existe
    has_ndaq = os.path.exists(ndaq_path)
    
    if has_ndaq:
        ndaq = spark.read.parquet(ndaq_path)
        ndaq_count = ndaq.count()
        print(f"[INFO] NASDAQ records: {ndaq_count}")
        
        if ndaq_count > 0:
            ndaq_renamed = ndaq.select(
                F.col("ts_minute_utc"),
                F.col("close").alias("ndaq_close"),
                F.col("volume").alias("ndaq_volume"),
                F.col("high").alias("ndaq_high"),
                F.col("low").alias("ndaq_low")
            )
            
            # Joindre BTC et NASDAQ
            joined = btc_renamed.join(ndaq_renamed, on="ts_minute_utc", how="left")
            print(f"[INFO] Joined records: {joined.count()}")
        else:
            has_ndaq = False
    
    if not has_ndaq:
        print(f"[WARNING] NASDAQ data not found or empty, using BTC only")
        joined = btc_renamed \
            .withColumn("ndaq_close", F.lit(None).cast("double")) \
            .withColumn("ndaq_volume", F.lit(None).cast("double")) \
            .withColumn("ndaq_high", F.lit(None).cast("double")) \
            .withColumn("ndaq_low", F.lit(None).cast("double"))

    # Créer les features lead-lag
    window_spec = Window.orderBy("ts_minute_utc")
    
    for lag in range(1, max_lag_minutes + 1):
        # BTC lag/lead features
        joined = joined.withColumn(f"btc_close_lag_{lag}", F.lag("btc_close", lag).over(window_spec))
        joined = joined.withColumn(f"btc_close_lead_{lag}", F.lead("btc_close", lag).over(window_spec))
        joined = joined.withColumn(f"btc_volume_lag_{lag}", F.lag("btc_volume", lag).over(window_spec))
        
        # NASDAQ lag/lead features (si disponible)
        if has_ndaq:
            joined = joined.withColumn(f"ndaq_close_lag_{lag}", F.lag("ndaq_close", lag).over(window_spec))
            joined = joined.withColumn(f"ndaq_close_lead_{lag}", F.lead("ndaq_close", lag).over(window_spec))

    # Calculer les returns (variation en %)
    joined = joined.withColumn(
        "btc_return_1m",
        F.when(F.col("btc_close_lag_1").isNotNull() & (F.col("btc_close_lag_1") != 0),
               (F.col("btc_close") - F.col("btc_close_lag_1")) / F.col("btc_close_lag_1") * 100)
        .otherwise(None)
    )
    
    if has_ndaq:
        joined = joined.withColumn(
            "ndaq_return_1m",
            F.when(F.col("ndaq_close_lag_1").isNotNull() & (F.col("ndaq_close_lag_1") != 0),
                   (F.col("ndaq_close") - F.col("ndaq_close_lag_1")) / F.col("ndaq_close_lag_1") * 100)
            .otherwise(None)
        )

    # Ajouter métadonnées
    joined = joined.withColumn("processed_at_utc", F.current_timestamp())
    joined = joined.withColumn("execution_date", F.lit(execution_date))

    # Supprimer les lignes où toutes les features lag sont nulles
    joined = joined.filter(F.col("btc_close_lag_1").isNotNull())

    print(f"[INFO] Final schema:")
    joined.printSchema()
    
    print(f"[INFO] Final record count: {joined.count()}")
    print(f"[INFO] Sample data:")
    joined.show(5, truncate=False)

    # Sauvegarder
    os.makedirs(output_path, exist_ok=True)
    
    pdf = joined.toPandas()
    table = pa.Table.from_pandas(pdf)
    parquet_file = os.path.join(output_path, "data.parquet")
    pq.write_table(table, parquet_file, coerce_timestamps='us', allow_truncated_timestamps=True)
    
    # Créer le fichier _SUCCESS
    success_file = os.path.join(output_path, "_SUCCESS")
    with open(success_file, "w") as f:
        pass
    
    print(f"[SUCCESS] Wrote {len(pdf)} lead-lag features to {output_path}")
    
    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(args.execution_date, args.max_lag_minutes)