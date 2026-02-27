import argparse
import os
import json
from datetime import datetime

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Index usage data to Elasticsearch")
    parser.add_argument("--execution_date", required=True, help="Date d'exécution (YYYY-MM-DD)")
    return parser.parse_args()


def get_elastic_config():
    """Configuration Elasticsearch depuis variables d'environnement."""
    return {
        "host": os.getenv("ELASTICSEARCH_HOST", "elasticsearch"),
        "port": os.getenv("ELASTICSEARCH_PORT", "9200"),
        "user": os.getenv("ELASTICSEARCH_USER", "elastic"),
        "password": os.getenv("ELASTICSEARCH_PASSWORD", "changeme"),
    }


def main(execution_date: str):
    print(f"[INFO] Indexing data for {execution_date}")
    
    # Chemin des données usage
    input_path = f"data/usage/finance/lead_lag_analysis/dt={execution_date}/data.parquet"
    
    if not os.path.exists(input_path):
        print(f"[ERROR] Usage data not found: {input_path}")
        raise FileNotFoundError(f"Usage data not found: {input_path}")

    # Lire les données
    df = pd.read_parquet(input_path)
    print(f"[INFO] Records to index: {len(df)}")

    if df.empty:
        print("[WARNING] No records to index")
        return

    # Configuration Elasticsearch
    config = get_elastic_config()
    es_url = f"http://{config['host']}:{config['port']}"
    index_name = f"btc-leadlag-{execution_date}"

    print(f"[INFO] Target index: {index_name}")

    try:
        from elasticsearch import Elasticsearch
        from elasticsearch.helpers import bulk

        # Connexion
        es = Elasticsearch(
            [es_url],
            basic_auth=(config["user"], config["password"]),
            verify_certs=False
        )

        # Vérifier la connexion
        if not es.ping():
            raise ConnectionError("Cannot connect to Elasticsearch")

        print("[INFO] Connected to Elasticsearch")

        # Préparer les documents
        def generate_docs():
            for idx, row in df.iterrows():
                doc = row.to_dict()
                # Convertir les timestamps en string ISO
                for key, value in doc.items():
                    if isinstance(value, pd.Timestamp):
                        doc[key] = value.isoformat()
                    elif pd.isna(value):
                        doc[key] = None
                
                yield {
                    "_index": index_name,
                    "_id": f"{execution_date}_{idx}",
                    "_source": doc
                }

        # Bulk indexing
        success, failed = bulk(es, generate_docs(), raise_on_error=False)
        
        print(f"[SUCCESS] Indexed {success} documents")
        if failed:
            print(f"[WARNING] Failed to index {len(failed)} documents")

    except ImportError:
        print("[WARNING] elasticsearch package not installed, saving to JSON fallback")
        
        # Fallback: sauvegarder en JSON
        fallback_path = f"data/export/elasticsearch_fallback/dt={execution_date}"
        os.makedirs(fallback_path, exist_ok=True)
        
        json_file = os.path.join(fallback_path, "lead_lag_features.json")
        
        # Convertir en JSON-serializable
        df_json = df.copy()
        for col in df_json.columns:
            if df_json[col].dtype == 'datetime64[ns]' or df_json[col].dtype == 'datetime64[ns, UTC]':
                df_json[col] = df_json[col].astype(str)
        
        df_json.to_json(json_file, orient="records", indent=2)
        print(f"[FALLBACK] Saved to JSON: {json_file}")

    except Exception as e:
        print(f"[ERROR] Failed to index to Elasticsearch: {e}")
        raise



if __name__ == "__main__":
    args = parse_args()
    main(args.execution_date)
