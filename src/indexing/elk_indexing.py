import os
import re
from datetime import date as dt
from typing import Tuple

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from glob import glob
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv

load_dotenv()

USAGE_DATA = "data/usage/finance/lead_lag_analysis/"

ELK_ENDPOINT = os.getenv("ELK_ENDPOINT")
ELK_API_KEY = os.getenv("ELK_API_KEY")

ELK_INDEX = "finance"

client = Elasticsearch(
    hosts=[ELK_ENDPOINT],
    api_key=ELK_API_KEY
)

def get_data_date(filepath: Path) -> Tuple[str | None, bool]:
    is_today = False
    parent_name = filepath.parent.name
    res = re.search(r'dt=(\d{4}-\d{2}-\d{2})', parent_name)
    date_str = res.group(1) if res else None
    if date_str:
        today = dt.today()
        is_today = dt.fromisoformat(date_str) == today
    return (date_str, is_today)
        
 
def generate_docs(df):
    for idx, row in df.iterrows():
        doc = row.to_dict()
        _id = doc["ts_minute_utc"].strftime("%Y-%m-%d %H:%M:%S")
        # Convertir les timestamps en string ISO
        for key, value in doc.items():
            if isinstance(value, pd.Timestamp):
                doc[key] = value.isoformat()
            elif pd.isna(value):
                doc[key] = None                
        yield {
            "_id": _id,
            "_source": doc
        }
                   
    
def elk_index(file_path: Path):
    # df = pd.read_parquet(file_path)
    df = pq.read_table(file_path).to_pandas()
    df.drop_duplicates(subset="ts_minute_utc", inplace=True)
    # records = df.dropna().to_dict(orient="records")
    records = generate_docs(df)
    try:
        success, _ = helpers.bulk(client, records, index=ELK_INDEX)
        print(f"\n{success} records indexed from {file_path} of {len(df)} rows.\n")
        _, is_today = get_data_date(file_path)
        if not is_today:
            success_file = file_path.parent.joinpath("_ELKSUCCESS")
            with open(success_file, "w"):
                pass
    except Exception as e:
        print("\n=== EXCEPTION THROWN ===")
        print(f"filepath: {file_path}")
        print(type(e), e)
        if hasattr(e, "errors"):
            print(e.errors[:7])
            print("\n=== ERROR DISPLAYED ABOVE ===\n")
            raise e
    
def content_to_index(data_path: str) -> list[Path]:
    results = []
    dir_list = glob(data_path  + "dt=*")
    print(f"\nNumber of folders to check in {data_path  + 'dt=*'}: {len(dir_list)}")
    for folder in dir_list:
        success_elk_indexation_file = Path(folder).joinpath("_ELKSUCCESS")
        # Check whether the directory content hasn't already been added
        # by looking at the absence of ELK successful index file
        if not success_elk_indexation_file.exists():
            results.append(Path(folder).joinpath("data.parquet"))
    return results

def main():
    to_ingest = content_to_index(USAGE_DATA)
    for filename in to_ingest:
        elk_index(filename)

if __name__ == "__main__":
    main()
