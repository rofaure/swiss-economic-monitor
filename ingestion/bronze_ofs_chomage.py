"""
OFS (Swiss Federal Statistical Office) unemployment ingestion module.
Fetches unemployment BIT CSV and returns a tagged Bronze DataFrame.
"""

import uuid
import pandas as pd
from datetime import datetime, timezone


def ingest_unemployment() -> pd.DataFrame:
    """
    Download OFS unemployment CSV and return a Bronze DataFrame.

    Returns:
        DataFrame with raw data + Bronze metadata columns
    """
    url = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/36519062/master"

    run_id      = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()

    df = pd.read_csv(url, sep=",", encoding="utf-8")

    df["_run_id"]      = run_id
    df["_ingested_at"] = ingested_at
    df["_source"]      = "ofs_unemployment_bit"
    df["_source_url"]  = url

    return df


if __name__ == "__main__":
    from pathlib import Path

    BRONZE_PATH = Path("data/bronze")
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    df = ingest_unemployment()
    df.to_parquet(BRONZE_PATH / "bronze_unemployment.parquet", index=False)
    print(f"bronze_unemployment : {len(df)} rows")