import os
import uuid
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from glob import glob
from pathlib import Path
import pyarrow.parquet as pq
from typing import Tuple
import re
from datetime import date as dt


BTC_RAW_DATA_PATH = "data/raw/finance/crypto/binance/btc_usdt/"
YFINANCE_RAW_DATA_PATH = "data/raw/market/yfinance/nasdaq_100/"
BTC_NASDAQ_USAGE_DATA_PATH = "data/usage/finance/lead_lag_analysis/"



def load_data(filepath: str, table_name: str) -> None:
	"""Load CSV data at `filepath` into Postgres `database_name`.`table_name`.

	Behavior:
	- Reads the file into a pandas DataFrame.
	- Writes the DataFrame to a temporary staging table.
	- If the target table exists and has a primary key, performs an
	  INSERT ... ON CONFLICT (...) DO UPDATE to upsert rows.
	- If the target table exists but has no primary key, inserts only
	  rows that do not already exist in the target (matching on all columns).
	- If the target table does not exist, creates it from the DataFrame.

	Connection settings are read from environment variables if present:
	`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.
	Defaults match the docker compose service `localhost`.
	 """

	host = os.getenv("POSTGRES_HOST", "datalake-warehouse")
	port = os.getenv("POSTGRES_PORT", "5433")
	user = os.getenv("POSTGRES_USER", "datalake_user")
	password = os.getenv("POSTGRES_PASSWORD", "datalake_pass")
	database_name = os.getenv("POSTGRES_DB", "datalake")
	engine = create_engine(
		f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database_name}"
	)

	# Get filepath extension
	ext = filepath.split(".")[-1]
	print(f"Extension of filepath {filepath}")
	if ext == "parquet":
		table = pq.read_table(filepath)
		df = table.to_pandas()
	elif ext == "csv":
		df = pd.read_csv(filepath)
	elif ext == "json":
		df = pd.read_json(filepath, orient="records")
	else:
		return

	if df.empty:
		return

	# Ensure partition column `dt` is present in the DataFrame.
	# Many writers store partition as parent folder name (dt=YYYY-MM-DD)
	# but not inside the parquet file itself. If missing, populate it
	# from the file path.
	try:
		date_str, _ = get_data_date(Path(filepath))
		if date_str and "dt" not in df.columns:
			df["dt"] = date_str
	except Exception:
		# Defensive: if something goes wrong extracting the date, continue
		pass

	# Use a unique staging table name to avoid collisions
	staging_table = f"_staging_{table_name}_{uuid.uuid4().hex[:8]}"

	with engine.begin() as conn:
		# Write staging table
		df.to_sql(staging_table, conn, index=False, if_exists="replace", method="multi")

		inspector = inspect(conn)

		# If target table does not exist, create it from the DataFrame and drop staging
		if not inspector.has_table(table_name):
			df.to_sql(table_name, conn, index=False, if_exists="replace", method="multi")
			conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))
			return

		# Get column list for the target table
		columns_info = inspector.get_columns(table_name)
		target_cols = [c["name"] for c in columns_info]

		# Get primary key columns (may be empty)
		pk = inspector.get_pk_constraint(table_name)
		pk_cols = pk.get("constrained_columns", []) if pk else []

		col_list = ", ".join([f'"{c}"' for c in target_cols])
		# When selecting from the staging table we must qualify columns with its alias
		select_list = ", ".join([f't."{c}"' for c in target_cols])

		if pk_cols:
			conflict_target = ", ".join([f'"{c}"' for c in pk_cols])
			update_set = ", ".join(
				[f'"{c}" = EXCLUDED."{c}"' for c in target_cols if c not in pk_cols]
			) or "DO NOTHING"

			sql = (
				f'INSERT INTO "{table_name}" ({col_list}) '
				f'SELECT {select_list} FROM "{staging_table}" t '
				f'ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set};'
			)
		else:
			# No primary key: insert rows from staging that do not already exist
			# Match on all columns using IS NOT DISTINCT FROM to handle NULLs
			match_conditions = " AND ".join(
				[f't."{c}" IS NOT DISTINCT FROM u."{c}"' for c in target_cols]
			)
			sql = (
				f'INSERT INTO "{table_name}" ({col_list}) '
				f'SELECT {select_list} FROM "{staging_table}" t '
				f'WHERE NOT EXISTS (SELECT 1 FROM "{table_name}" u WHERE {match_conditions});'
			)

		conn.execute(text(sql))
		conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}"'))


def content_to_index(data_path: str) -> list[Path]:
	results = []
	dir_list = glob(data_path  + "dt=*")
	print(f"\nNumber of folders to check in {data_path  + 'dt=*'}: {len(dir_list)}")
	for folder in dir_list:
		success_postgres_ingestion_file = Path(folder).joinpath("_POSTGRES")
		# Check whether the directory content hasn't already been added
		# by looking at the absence of ELK successful index file
		if not success_postgres_ingestion_file.exists():
			# Only include data files with supported extensions and skip marker files
			files_paths = glob(folder + "/*.*")
			allowed = {".parquet", ".csv", ".json"}
			filtered = [p for p in files_paths if Path(p).suffix.lower() in allowed and not Path(p).name.startswith("_")]
			results.extend(filtered)
	return results


def get_data_date(filepath: Path) -> Tuple[str | None, bool]:
    is_today = False
    parent_name = Path(filepath).parent.name
    res = re.search(r'dt=(\d{4}-\d{2}-\d{2})', parent_name)
    date_str = res.group(1) if res else None
    if date_str:
        today = dt.today()
        is_today = dt.fromisoformat(date_str) == today
    return (date_str, is_today)


def load_data_in_postgres(dir_path, table_name):
    files = content_to_index(dir_path)
    for file in files:
        load_data(file, table_name)
        _, is_today = get_data_date(file)
        if not is_today:
            success_postgres_ingestion_file = Path(file).parent.joinpath("_POSTGRES")
            with open(success_postgres_ingestion_file, "w"):
                pass

if __name__ == "__main__":
    # load_data_in_postgres(BTC_RAW_DATA_PATH, "raw_btc")
    # load_data_in_postgres(YFINANCE_RAW_DATA_PATH, "raw_nasdaq")
    load_data_in_postgres(BTC_NASDAQ_USAGE_DATA_PATH, "btc_nasdaq")