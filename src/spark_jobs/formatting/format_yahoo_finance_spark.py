import argparse
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--execution_date", required=True, help="YYYY-MM-DD (Airflow ds)")
    return p.parse_args()


def main(execution_date: str):
    spark = (
        SparkSession.builder
        .appName("format_yahoo_finance_ndaq")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")

    # CORRECTION: Chemin raw corrigé (market au lieu de finance/market)
    raw_path = f"data/raw/market/yfinance/nasdaq_100/dt={execution_date}/nasdaq_ohlcv.csv"
    # CORRECTION: Chemin output cohérent avec le reste du projet
    out_path = f"data/formatted/market/yfinance/nasdaq_100/dt={execution_date}"

    print(f"[INFO] Reading from: {raw_path}")
    print(f"[INFO] Writing to: {out_path}")

    # Vérifier si le fichier existe
    if not os.path.exists(raw_path):
        print(f"[WARNING] Raw file not found: {raw_path}")
        # Créer un DataFrame vide pour éviter l'échec
        spark.stop()
        return

    # Read raw CSV
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )
    
    print(f"[INFO] Raw records: {df.count()}")
    print("[INFO] Raw schema:")
    df.printSchema()

    # Normalize / cast
    df2 = (
        df
        .withColumn("ts_minute_utc", F.to_timestamp("ts_minute_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("ingested_at_utc", F.to_timestamp("ingested_at_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("datetime_utc", F.to_timestamp(F.col("datetime")))
        .withColumn("open", F.col("open").cast("double"))
        .withColumn("high", F.col("high").cast("double"))
        .withColumn("low", F.col("low").cast("double"))
        .withColumn("close", F.col("close").cast("double"))
        .withColumn("volume", F.col("volume").cast("double"))
        .withColumn("symbol", F.col("symbol").cast("string"))
        .withColumn("source", F.col("source").cast("string"))
        .withColumn("interval", F.col("interval").cast("string"))
        # Colonnes dérivées
        .withColumn("price_range", F.col("high") - F.col("low"))
        .withColumn("price_change", F.col("close") - F.col("open"))
        .withColumn("price_change_pct", 
                    F.when(F.col("open") != 0, 
                           (F.col("close") - F.col("open")) / F.col("open") * 100)
                    .otherwise(0))
        .withColumn("dt", F.lit(execution_date))
        .withColumn("formatted_at_utc", F.current_timestamp())
        .select(
            "dt",
            "ts_minute_utc",
            "datetime_utc",
            "open", "high", "low", "close", "volume",
            "price_range", "price_change", "price_change_pct",
            "symbol", "interval", "source", 
            "ingested_at_utc", "formatted_at_utc"
        )
        .dropna(subset=["ts_minute_utc", "open", "close"])
        .dropDuplicates(["ts_minute_utc", "symbol"])
        .orderBy("ts_minute_utc")
    )

    print(f"[INFO] Formatted records: {df2.count()}")
    print("[INFO] Sample data:")
    df2.show(5, truncate=False)

    # Écrire avec pandas+pyarrow
    os.makedirs(out_path, exist_ok=True)
    
    pdf = df2.toPandas()
    table = pa.Table.from_pandas(pdf)
    parquet_file = os.path.join(out_path, "data.parquet")
    pq.write_table(table, parquet_file, coerce_timestamps='us', allow_truncated_timestamps=True)
    
    # Créer le fichier _SUCCESS
    success_file = os.path.join(out_path, "_SUCCESS")
    with open(success_file, "w") as f:
        pass

    print(f"[OK] Wrote formatted parquet to: {out_path}")
    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(args.execution_date)