"""
Silver transformation for SNB policy rate data.
"""

from pathlib import Path
import pandas as pd


def transform_policy_rate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform bronze policy rate data to silver.
    Reconstructs continuous Swiss rate: LIBOR midpoint (2010-2019) + SNB rate (2019+).
    Returns (accepted, rejected) DataFrames.
    """
    rejected_rows = []

    # ── Keep Swiss series only ───────────────────────────────────────────────
    swiss_mask = df["_series_name"].str.startswith("Suisse")
    rejected_rows.append(df[~swiss_mask].assign(_rejection_reason="Non-Swiss central bank"))
    df_swiss = df[swiss_mask].copy()

    # ── Split into 3 series ──────────────────────────────────────────────────
    df_rate  = df_swiss[df_swiss["_series_name"].str.contains("Taux directeur de la BNS")]
    df_lower = df_swiss[df_swiss["_series_name"].str.contains("limite inférieure")]
    df_upper = df_swiss[df_swiss["_series_name"].str.contains("limite supérieure")]

    # ── Reconstruct 2010-2019: LIBOR midpoint ────────────────────────────────
    df_libor = df_lower[["date", "value", "_run_id", "_ingested_at", "_source", "_source_url"]].copy()
    df_libor = df_libor.merge(
        df_upper[["date", "value"]].rename(columns={"value": "value_upper"}),
        on="date"
    )
    df_libor["swiss_policy_rate"] = (df_libor["value"] + df_libor["value_upper"]) / 2
    df_libor = df_libor[(df_libor["date"] >= "2010-01") & (df_libor["date"] < "2019-06")]
    df_libor = df_libor[["date", "swiss_policy_rate", "_run_id", "_ingested_at", "_source", "_source_url"]]
    df_libor["_rate_source"] = "LIBOR_midpoint"

    # ── 2019-2026: SNB policy rate ───────────────────────────────────────────
    df_snb = df_rate[df_rate["date"] >= "2019-06"][["date", "value", "_run_id", "_ingested_at", "_source", "_source_url"]].copy()
    df_snb = df_snb.rename(columns={"value": "swiss_policy_rate"})
    df_snb["_rate_source"] = "SNB_policy_rate"

    # ── Concatenate both periods ─────────────────────────────────────────────
    df_final = pd.concat([df_libor, df_snb], ignore_index=True)
    df_final = df_final.rename(columns={"date": "period"})
    df_final = df_final.sort_values("period").reset_index(drop=True)

    rejected = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()

    return df_final, rejected


if __name__ == "__main__":
    BRONZE_PATH = Path("data/bronze")
    SILVER_PATH = Path("data/silver")
    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    df_bronze = pd.read_parquet(BRONZE_PATH / "bronze_policy_rate.parquet")
    accepted, rejected = transform_policy_rate(df_bronze)

    accepted.to_parquet(SILVER_PATH / "silver_policy_rate.parquet", index=False)
    rejected.to_parquet(SILVER_PATH / "silver_policy_rate_rejected.parquet", index=False)

    print(f"Accepted : {len(accepted)} rows")
    print(f"Rejected : {len(rejected)} rows")