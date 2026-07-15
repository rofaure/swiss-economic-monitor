"""
SNB (Swiss National Bank) API ingestion module.
Fetches any SNB data cube and returns a tagged Bronze DataFrame.
"""

import uuid
import requests
import pandas as pd
from datetime import datetime, timezone


def ingest_snb_cube(cube_id: str, source_name: str) -> pd.DataFrame:
    """
    Fetch a SNB data cube via REST API and return a Bronze DataFrame.

    Args:
        cube_id    : SNB cube identifier (e.g. 'plkopr', 'snboffzisa')
        source_name: human-readable source label for lineage tracking

    Returns:
        DataFrame with all timeseries flattened + Bronze metadata columns
    """
    url = f"https://data.snb.ch/api/cube/{cube_id}/data/json/fr"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    run_id      = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()

    dfs = []
    for ts in response.json()["timeseries"]:
        df_ts = pd.DataFrame(ts["values"])
        df_ts["_series_key"]  = ts["metadata"]["key"]
        df_ts["_series_name"] = ts["header"][0]["dimItem"]
        dfs.append(df_ts)

    df = pd.concat(dfs, ignore_index=True)
    df["_run_id"]      = run_id
    df["_ingested_at"] = ingested_at
    df["_source"]      = source_name
    df["_source_url"]  = url

    return df


if __name__ == "__main__":
    from pathlib import Path

    BRONZE_PATH = Path("data/bronze")
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    df_cpi = ingest_snb_cube("plkopr", "snb_cpi")
    df_cpi.to_parquet(BRONZE_PATH / "bronze_cpi.parquet", index=False)
    print(f"bronze_cpi          : {len(df_cpi)} rows")

    df_policy = ingest_snb_cube("snboffzisa", "snb_policy_rate")
    df_policy.to_parquet(BRONZE_PATH / "bronze_policy_rate.parquet", index=False)
    print(f"bronze_policy_rate  : {len(df_policy)} rows")