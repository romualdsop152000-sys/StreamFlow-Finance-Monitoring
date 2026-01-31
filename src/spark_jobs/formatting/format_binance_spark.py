import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


RAW_BASE = "data/raw/finance/crypto/binance/btc_usdt"
FMT_BASE = "data/formatted/finance/crypto/btc_usdt"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--execution_date", required=False, help="YYYY-MM-DD (UTC). Default: today UTC")
    return p.parse_args()


def utc_today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def main():
    args = parse_args()
    dt = args.execution_date or utc_today()

    raw_dir = Path(RAW_BASE) / f"dt={dt}"
    raw_file_jsonl = raw_dir / "btc_usdt_1min.jsonl"
    raw_file_json = raw_dir / "btc_usdt_1min.json"  # fallback si vous utilisez .json

    if raw_file_jsonl.exists():
        input_path = str(raw_file_jsonl)
    elif raw_file_json.exists():
        input_path = str(raw_file_json)
    else:
        raise FileNotFoundError(
            f"No raw input found for dt={dt}. Expected {raw_file_jsonl} or {raw_file_json}"
        )

    out_dir = Path(FMT_BASE) / f"dt={dt}"
    out_path = str(out_dir / "btc_usdt_1min.parquet")

    spark = build_spark(f"format_binance_btc_usdt_1m_{dt}")

    # Schéma attendu (robuste + types stricts)
    schema = T.StructType([
        T.StructField("open_time_ms", T.LongType(), True),
        T.StructField("open_time_utc", T.StringType(), True),

        # Optionnel : si tu as déjà ajouté ts_minute_utc dans raw
        T.StructField("ts_minute_utc", T.StringType(), True),

        T.StructField("open", T.DoubleType(), True),
        T.StructField("high", T.DoubleType(), True),
        T.StructField("low", T.DoubleType(), True),
        T.StructField("close", T.DoubleType(), True),
        T.StructField("volume", T.DoubleType(), True),

        T.StructField("close_time_ms", T.LongType(), True),
        T.StructField("close_time_utc", T.StringType(), True),

        T.StructField("quote_asset_volume", T.DoubleType(), True),
        T.StructField("number_of_trades", T.LongType(), True),
        T.StructField("taker_buy_base_asset_volume", T.DoubleType(), True),
        T.StructField("taker_buy_quote_asset_volume", T.DoubleType(), True),

        T.StructField("symbol", T.StringType(), True),
        T.StructField("interval", T.StringType(), True),
        T.StructField("ingested_at_utc", T.StringType(), True),
        T.StructField("source", T.StringType(), True),
    ])

    # Lecture JSONL (Spark lit bien un json par ligne)
    df = spark.read.schema(schema).json(input_path)

    # Normalisation timestamp: ts_minute_utc = open_time_utc arrondi à la minute (UTC)
    # - si ts_minute_utc existe déjà => on le garde
    # - sinon on le construit depuis open_time_ms ou open_time_utc
    df = df.withColumn(
        "ts_minute_utc",
        F.when(
            F.col("ts_minute_utc").isNotNull(),
            F.col("ts_minute_utc")
        ).otherwise(
            # Priorité open_time_ms (plus fiable), fallback open_time_utc
            F.when(
                F.col("open_time_ms").isNotNull(),
                F.date_format(
                    (F.col("open_time_ms") / F.lit(1000)).cast("timestamp"),
                    "yyyy-MM-dd'T'HH:mm:00'Z'"
                )
            ).otherwise(
                # Si open_time_utc est ISO, on le parse puis on arrondit
                F.date_format(
                    F.to_timestamp(F.col("open_time_utc")),
                    "yyyy-MM-dd'T'HH:mm:00'Z'"
                )
            )
        )
    )

    # Nettoyage minimal (valeurs critiques)
    df = (
        df.filter(F.col("symbol") == F.lit("BTCUSDT"))
          .filter(F.col("open").isNotNull() & F.col("close").isNotNull())
    )

    # Ajout partition dt (utile en parquet + pour requêtes)
    df = df.withColumn("dt", F.lit(dt))

    # Sélection + ordre clair (schéma final)
    out = df.select(
        "dt",
        "ts_minute_utc",
        "open_time_ms", "close_time_ms",
        "open", "high", "low", "close", "volume",
        "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
        "symbol", "interval",
        "source", "ingested_at_utc"
    )

    # Écriture en parquet (un dossier parquet, standard Spark)
    # On écrit dans out_dir (Spark crée des part-*.parquet)
    out_dir.mkdir(parents=True, exist_ok=True)
    out.write.mode("overwrite").parquet(str(out_dir))

    print(f"[OK] Wrote formatted parquet to: {out_dir}")
    spark.stop()


if __name__ == "__main__":
    main()
