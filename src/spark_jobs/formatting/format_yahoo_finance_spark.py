import argparse
from pathlib import Path

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
        .getOrCreate()
    )

    raw_path = f"data/raw/market/yfinance/nasdaq_100/dt={execution_date}/nasdaq_ohlcv.csv"
    out_path = f"data/formatted/finance/market/nasdaq_100/dt={execution_date}"

    # Read raw CSV
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )

    # Normalize / cast
    # Expect columns: Datetime, open, high, low, close, volume, ts_minute_utc, symbol, source, ingested_at_utc, interval
    df2 = (
        df
        # Parse timestamps (ts_minute_utc is "YYYY-MM-DDTHH:MM:SSZ")
        .withColumn("ts_minute_utc", F.to_timestamp("ts_minute_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("ingested_at_utc", F.to_timestamp("ingested_at_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("datetime_utc", F.to_timestamp(F.col("Datetime")))
        # Ensure numeric types
        .withColumn("open", F.col("open").cast("double"))
        .withColumn("high", F.col("high").cast("double"))
        .withColumn("low", F.col("low").cast("double"))
        .withColumn("close", F.col("close").cast("double"))
        .withColumn("volume", F.col("volume").cast("double"))
        .withColumn("symbol", F.col("symbol").cast("string"))
        .withColumn("source", F.col("source").cast("string"))
        .withColumn("interval", F.col("interval").cast("string"))
        .withColumn("dt", F.lit(execution_date))
        .select(
            "dt",
            "ts_minute_utc",
            "open", "high", "low", "close", "volume",
            "symbol", "interval", "source", "ingested_at_utc",
            "datetime_utc"
        )
        .dropna(subset=["ts_minute_utc", "open", "close"])
        .dropDuplicates(["ts_minute_utc", "symbol"])
        .orderBy("ts_minute_utc")
    )

    Path(out_path).mkdir(parents=True, exist_ok=True)

    (df2.write
        .mode("overwrite")
        .parquet(out_path)
    )

    print(f"[OK] Wrote formatted parquet to: {out_path}")
    spark.stop()


if __name__ == "__main__":
    args = parse_args()
    main(args.execution_date)
