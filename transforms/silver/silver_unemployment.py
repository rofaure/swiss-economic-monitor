"""
Silver transformation for OFS unemployment data.
"""

import pandas as pd
from pathlib import Path


def transform_unemployment(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform bronze unemployment data to silver.
    Returns (accepted, rejected) DataFrames.
    """
    rejected_rows = []

    mask_freq = df["FREQ"] == "M"
    rejected_rows.append(df[~mask_freq].assign(_rejection_reason="FREQ != M"))
    df = df[mask_freq].copy()

    mask_period = df["PERIOD"] >= "2010-M01"
    rejected_rows.append(df[~mask_period].assign(_rejection_reason="PERIOD < 2010-M01"))
    df = df[mask_period].copy()

    mask_gender = df["GENDER_FR"] == "Total"
    rejected_rows.append(df[~mask_gender].assign(_rejection_reason="GENDER_FR != Total"))
    df = df[mask_gender].copy()

    mask_nuts = df["INDICATORS_FR"] == "NUTS2"
    rejected_rows.append(df[~mask_nuts].assign(_rejection_reason="INDICATORS_FR != NUTS2"))
    df = df[mask_nuts].copy()

    df["period"] = df["PERIOD"].str.replace("-M", "-", regex=False)

    cols_to_drop = ["INDICATORS_HRCHY", "INDICATORS_DE", "GENDER_DE",
                    "DETAILS_DE", "FREQ", "MEASURE_FR", "MEASURE_DE",
                    "STATUS", "STATUS_1", "PERIOD"]
    df = df.drop(columns=cols_to_drop)

    df = df.rename(columns={
        "INDICATORS_FR" : "indicator",
        "GENDER_FR"     : "gender",
        "DETAILS_FR"    : "region",
        "VALUE"         : "unemployment_rate"
    })

    rejected = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()

    return df, rejected


if __name__ == "__main__":
    BRONZE_PATH = Path("data/bronze")
    SILVER_PATH = Path("data/silver")
    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.read_parquet(BRONZE_PATH / "bronze_unemployment.parquet")
    accepted, rejected = transform_unemployment(df_bronze)

    accepted.to_parquet(SILVER_PATH / "silver_unemployment.parquet", index=False)
    rejected.to_parquet(SILVER_PATH / "silver_unemployment_rejected.parquet", index=False)

    print(f"Accepted : {len(accepted)} rows")
    print(f"Rejected : {len(rejected)} rows")