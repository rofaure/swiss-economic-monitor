"""
Silver transformation for OFS CPI data.
"""

from pathlib import Path
import pandas as pd


def transform_cpi(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform bronze CPI data to silver.
    Returns (accepted, rejected) DataFrames.
    """
    rejected_rows = []

    # ── Filter date >= 2010-01 ───────────────────────────────────────────────
    mask_date = df["date"] >= "2010-01"
    rejected_rows.append(df[~mask_date].assign(_rejection_reason="date < 2010-01"))
    df = df[mask_date].copy()

    # ── Extract series key short name ────────────────────────────────────────
    df["series_key"] = df["_series_key"].str.extract(r'\{(\w+)\}')

    # ── Rename columns ───────────────────────────────────────────────────────
    df = df.rename(columns={
        "date"  : "period",
        "value" : "cpi_value"
    })

    # ── Drop redundant columns ───────────────────────────────────────────────
    df = df.drop(columns=["_series_key", "_series_name"])

    rejected = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()

    return df, rejected

if __name__ == "__main__":
    BRONZE_PATH = Path("data/bronze")
    SILVER_PATH = Path("data/silver")
    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.read_parquet(BRONZE_PATH / "bronze_cpi.parquet")
    accepted, rejected = transform_cpi(df_bronze)

    accepted.to_parquet(SILVER_PATH / "silver_cpi.parquet", index=False)
    rejected.to_parquet(SILVER_PATH / "silver_cpi_rejected.parquet", index=False)

    print(f"Accepted : {len(accepted)} rows")
    print(f"Rejected : {len(rejected)} rows")