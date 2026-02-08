import argparse
import os
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType


def parse_args():
    parser = argparse.ArgumentParser(description="Format Binance BTC/USDT raw data")
    parser.add_argument("--execution_date", required=True, help="Date d'exécution (YYYY-MM-DD)")
    return parser.parse_args()


def get_spark_session():
    return SparkSession.builder \
        .appName("format_binance_btcusdt") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()


def read_raw_data(spark, dt: str):
    """
    Lit les données brutes depuis CSV, JSONL ou JSON.
    """
    base_path = "data/raw/finance/crypto/binance/btc_usdt"
    raw_dir = f"{base_path}/dt={dt}"
    
    csv_path = f"{raw_dir}/btc_usdt_1min.csv"
    jsonl_path = f"{raw_dir}/btc_usdt_1min.jsonl"
    json_path = f"{raw_dir}/btc_usdt_1min.json"

    if os.path.exists(csv_path):
        input_path = csv_path
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(input_path)
        print(f"[INFO] Reading CSV from: {input_path}")
    elif os.path.exists(jsonl_path):
        input_path = jsonl_path
        df = spark.read.json(input_path)
        print(f"[INFO] Reading JSONL from: {input_path}")
    elif os.path.exists(json_path):
        input_path = json_path
        df = spark.read.option("multiLine", "true").json(input_path)
        print(f"[INFO] Reading JSON from: {input_path}")
    else:
        raise FileNotFoundError(
            f"No raw input found for dt={dt}. Expected:\n"
            f"  - {csv_path}\n"
            f"  - {jsonl_path}\n"
            f"  - {json_path}"
        )

    return df


def format_dataframe(df):
    """
    Applique les transformations de formatage sur le DataFrame.
    - Cast des colonnes numériques
    - Parsing des timestamps
    - Ajout de colonnes dérivées
    """
    # Cast des colonnes numériques
    df = df \
        .withColumn("open", F.col("open").cast(DoubleType())) \
        .withColumn("high", F.col("high").cast(DoubleType())) \
        .withColumn("low", F.col("low").cast(DoubleType())) \
        .withColumn("close", F.col("close").cast(DoubleType())) \
        .withColumn("volume", F.col("volume").cast(DoubleType())) \
        .withColumn("quote_asset_volume", F.col("quote_asset_volume").cast(DoubleType())) \
        .withColumn("number_of_trades", F.col("number_of_trades").cast(LongType())) \
        .withColumn("taker_buy_base_asset_volume", F.col("taker_buy_base_asset_volume").cast(DoubleType())) \
        .withColumn("taker_buy_quote_asset_volume", F.col("taker_buy_quote_asset_volume").cast(DoubleType())) \
        .withColumn("open_time_ms", F.col("open_time_ms").cast(LongType())) \
        .withColumn("close_time_ms", F.col("close_time_ms").cast(LongType()))

    # Parsing des timestamps UTC
    df = df \
        .withColumn("open_time_utc", F.to_timestamp("open_time_utc")) \
        .withColumn("close_time_utc", F.to_timestamp("close_time_utc")) \
        .withColumn("ts_minute_utc", F.to_timestamp("ts_minute_utc"))

    # Colonnes dérivées
    df = df \
        .withColumn("price_range", F.col("high") - F.col("low")) \
        .withColumn("price_change", F.col("close") - F.col("open")) \
        .withColumn("price_change_pct", 
                    F.when(F.col("open") != 0, 
                           (F.col("close") - F.col("open")) / F.col("open") * 100)
                    .otherwise(0)) \
        .withColumn("is_bullish", F.col("close") > F.col("open")) \
        .withColumn("formatted_at_utc", F.current_timestamp())

    # Réordonner les colonnes
    columns_order = [
        "ts_minute_utc",
        "symbol",
        "interval",
        "open_time_ms",
        "open_time_utc",
        "close_time_ms",
        "close_time_utc",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "price_range",
        "price_change",
        "price_change_pct",
        "is_bullish",
        "source",
        "ingested_at_utc",
        "formatted_at_utc"
    ]

    # Ne garder que les colonnes existantes
    existing_columns = [c for c in columns_order if c in df.columns]
    df = df.select(existing_columns)

    return df


def write_formatted_data(df, dt: str):
    """
    Écrit les données formatées en Parquet.
    Utilise pandas+pyarrow pour contourner les problèmes de permissions Hadoop sur WSL.
    """
    output_path = f"data/formatted/finance/crypto/binance/btc_usdt/dt={dt}"
    
    # Créer le répertoire de sortie
    os.makedirs(output_path, exist_ok=True)
    
    # Convertir Spark DataFrame en pandas puis écrire avec pyarrow
    pdf = df.toPandas()
    table = pa.Table.from_pandas(pdf)
    parquet_file = os.path.join(output_path, "data.parquet")
    # Utiliser microseconds pour compatibilité avec Spark
    pq.write_table(table, parquet_file, coerce_timestamps='us', allow_truncated_timestamps=True)
    
    # Créer le fichier _SUCCESS pour indiquer la fin du job
    success_file = os.path.join(output_path, "_SUCCESS")
    with open(success_file, "w") as f:
        pass
    
    print(f"[OK] Wrote formatted data to {output_path}")
    return output_path


def main():
    args = parse_args()
    dt = args.execution_date

    print(f"[INFO] Starting Binance formatting job for dt={dt}")

    # Initialiser Spark
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Lire les données brutes
        df_raw = read_raw_data(spark, dt)
        print(f"[INFO] Raw records count: {df_raw.count()}")
        
        # Afficher le schéma initial
        print("[INFO] Raw schema:")
        df_raw.printSchema()

        # Formater les données
        df_formatted = format_dataframe(df_raw)
        print(f"[INFO] Formatted records count: {df_formatted.count()}")
        
        # Afficher le schéma formaté
        print("[INFO] Formatted schema:")
        df_formatted.printSchema()

        # Aperçu des données
        print("[INFO] Sample data:")
        df_formatted.show(5, truncate=False)

        # Écrire les données formatées
        output_path = write_formatted_data(df_formatted, dt)

        print(f"[SUCCESS] Binance formatting completed for dt={dt}")

    except Exception as e:
        print(f"[ERROR] Binance formatting failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()