# Fabric Notebook — Bronze Ingestion
# Run in Microsoft Fabric (PySpark kernel)
# Writes raw data to SEM_LH/Files/bronze/ as Parquet

import requests
import uuid
from datetime import datetime, timezone
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ── OFS Unemployment ──────────────────────────────────────────────────────────

url_unemp = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/36519062/master"
run_id = str(uuid.uuid4())
ingested_at = datetime.now(timezone.utc).isoformat()

df_unemp_pd = pd.read_csv(url_unemp, sep=",", encoding="utf-8")
df_unemp_pd["_run_id"] = run_id
df_unemp_pd["_ingested_at"] = ingested_at
df_unemp_pd["_source"] = "ofs_unemployment_bit"
df_unemp_pd["_source_url"] = url_unemp

df_unemp = spark.createDataFrame(df_unemp_pd)
df_unemp.write.mode("append").parquet("Files/bronze/bronze_unemployment")

print(f"[unemployment] run_id={run_id}")
print(f"[unemployment] rows written: {df_unemp.count()}")

# ── SNB Cubes (CPI + Policy Rate) ─────────────────────────────────────────────

def ingest_snb_cube(cube_id: str, source_name: str) -> pd.DataFrame:
    url = f"https://data.snb.ch/api/cube/{cube_id}/data/json/fr"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    run_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).isoformat()
    dfs = []
    for ts in response.json()["timeseries"]:
        df_ts = pd.DataFrame(ts["values"])
        df_ts["_series_key"] = ts["metadata"]["key"]
        df_ts["_series_name"] = ts["header"][0]["dimItem"]
        dfs.append(df_ts)
    df = pd.concat(dfs, ignore_index=True)
    df["_run_id"] = run_id
    df["_ingested_at"] = ingested_at
    df["_source"] = source_name
    df["_source_url"] = url
    return df

# CPI
df_cpi = spark.createDataFrame(ingest_snb_cube("plkopr", "snb_cpi"))
df_cpi.write.mode("append").parquet("Files/bronze/bronze_cpi")
print(f"[cpi] rows written: {df_cpi.count()}")

# Policy Rate
df_rate = spark.createDataFrame(ingest_snb_cube("snboffzisa", "snb_policy_rate"))
df_rate.write.mode("append").parquet("Files/bronze/bronze_policy_rate")
print(f"[policy_rate] rows written: {df_rate.count()}")
