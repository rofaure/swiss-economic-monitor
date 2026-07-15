"""
Gold layer transformation - builds dimensional model from silver data.
"""

from pathlib import Path
import pandas as pd


def build_dim_date(periods: pd.Series) -> pd.DataFrame:
    """Build DimDate from a series of YYYY-MM period strings."""
    unique_periods = periods.drop_duplicates().sort_values().reset_index(drop=True)
    dim_date = pd.DataFrame({"period": unique_periods})
    dim_date["date_id"]          = dim_date["period"].str.replace("-", "").astype(int)
    dim_date["year"]             = dim_date["period"].str[:4].astype(int)
    dim_date["month"]            = dim_date["period"].str[5:7].astype(int)
    dim_date["quarter"]          = ((dim_date["month"] - 1) // 3 + 1).astype(int)
    dim_date["year_month_label"] = dim_date["period"]
    return dim_date[["date_id", "period", "year", "month", "quarter", "year_month_label"]]


def build_dim_region(df: pd.DataFrame) -> pd.DataFrame:
    """Build DimRegion with GPS coordinates."""
    region_meta = {
        "Région lémanique"    : (46.3167, 6.5833, "Geneva, Lausanne, Sion"),
        "Espace Mittelland"   : (46.9833, 7.2167, "Bern, Fribourg, Neuchâtel"),
        "Suisse du Nord-Ouest": (47.5583, 7.5729, "Basel, Aarau, Soleure"),
        "Zurich"              : (47.3769, 8.5417, "Zurich, Winterthur"),
        "Suisse orientale"    : (47.4245, 9.3767, "St-Gallen, Chur, Schaffhausen"),
        "Suisse centrale"     : (47.0502, 8.3093, "Lucerne, Zug, Altdorf"),
        "Tessin"              : (46.0037, 8.9511, "Lugano, Bellinzona, Locarno"),
    }
    unique_regions = df["region"].drop_duplicates().sort_values().reset_index(drop=True)
    dim_region = pd.DataFrame({"region_name": unique_regions})
    dim_region["region_id"]   = range(1, len(dim_region) + 1)
    dim_region["latitude"]    = dim_region["region_name"].map(lambda x: region_meta[x][0])
    dim_region["longitude"]   = dim_region["region_name"].map(lambda x: region_meta[x][1])
    dim_region["main_cities"] = dim_region["region_name"].map(lambda x: region_meta[x][2])
    return dim_region[["region_id", "region_name", "latitude", "longitude", "main_cities"]]


def build_fact_unemployment(df: pd.DataFrame,
                             dim_date: pd.DataFrame,
                             dim_region: pd.DataFrame) -> pd.DataFrame:
    """Build FactUnemployment with dimension keys."""
    fact = df.copy()
    fact = fact.merge(dim_date[["period", "date_id"]], on="period", how="left")
    fact = fact.merge(dim_region[["region_name", "region_id"]],
                      left_on="region", right_on="region_name", how="left")
    return fact[["date_id", "region_id", "unemployment_rate", "_run_id", "_ingested_at"]]


def build_fact_macro(df_cpi: pd.DataFrame,
                     df_policy: pd.DataFrame,
                     dim_date: pd.DataFrame) -> pd.DataFrame:
    """Build FactMacroIndicators with CPI and SNB policy rate."""
    cpi_pivot = df_cpi.pivot(index="period", columns="series_key", values="cpi_value").reset_index()
    cpi_pivot = cpi_pivot.rename(columns={"LD2010100": "cpi_index", "VVP": "cpi_yoy"})
    fact = cpi_pivot.merge(df_policy[["period", "swiss_policy_rate", "_rate_source"]], on="period", how="inner")
    fact = fact.merge(dim_date[["period", "date_id"]], on="period", how="left")
    return fact[["date_id", "period", "cpi_index", "cpi_yoy", "swiss_policy_rate", "_rate_source"]].sort_values("period").reset_index(drop=True)


if __name__ == "__main__":
    SILVER_PATH = Path("data/silver")
    GOLD_PATH   = Path("data/gold")
    GOLD_PATH.mkdir(parents=True, exist_ok=True)

    df_unemployment = pd.read_parquet(SILVER_PATH / "silver_unemployment.parquet")
    df_cpi          = pd.read_parquet(SILVER_PATH / "silver_cpi.parquet")
    df_policy       = pd.read_parquet(SILVER_PATH / "silver_policy_rate.parquet")

    all_periods    = pd.concat([df_unemployment["period"], df_cpi["period"], df_policy["period"]])
    dim_date       = build_dim_date(all_periods)
    dim_region     = build_dim_region(df_unemployment)
    fact_unemployment = build_fact_unemployment(df_unemployment, dim_date, dim_region)
    fact_macro        = build_fact_macro(df_cpi, df_policy, dim_date)

    dim_date.to_parquet(GOLD_PATH / "dim_date.parquet", index=False)
    dim_region.to_parquet(GOLD_PATH / "dim_region.parquet", index=False)
    fact_unemployment.to_parquet(GOLD_PATH / "fact_unemployment.parquet", index=False)
    fact_macro.to_parquet(GOLD_PATH / "fact_macro_indicators.parquet", index=False)

    print(f"dim_date              : {len(dim_date)} rows")
    print(f"dim_region            : {len(dim_region)} rows")
    print(f"fact_unemployment     : {len(fact_unemployment)} rows")
    print(f"fact_macro_indicators : {len(fact_macro)} rows")