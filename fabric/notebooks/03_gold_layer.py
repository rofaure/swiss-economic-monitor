# Fabric Notebook — Gold Layer
# Run in Microsoft Fabric (PySpark kernel)
# Reads Silver Delta tables, writes Gold Delta tables (star schema)

from pyspark.sql.functions import col, lit, concat, regexp_extract
from pyspark.sql.types import (StructType, StructField,
                                IntegerType, StringType, DoubleType)

# ── DimDate ───────────────────────────────────────────────────────────────────

df_unemp = spark.read.table("silver_unemployment")
df_macro  = spark.read.table("silver_cpi")

periods = (df_unemp.select("period")
    .union(df_macro.select("period"))
    .distinct()
    .orderBy("period"))

dim_date = (periods
    .withColumn("date_id",
        (col("period").substr(1, 4).cast(IntegerType()) * 100 +
         col("period").substr(6, 2).cast(IntegerType())))
    .withColumn("year",  col("period").substr(1, 4).cast(IntegerType()))
    .withColumn("month", col("period").substr(6, 2).cast(IntegerType()))
    .withColumn("quarter", ((col("month") - 1) / 3 + 1).cast(IntegerType()))
    .withColumn("year_month_label",
        concat(col("period").substr(6, 2), lit("/"), col("period").substr(1, 4))))

dim_date.write.mode("overwrite").format("delta").saveAsTable("dim_date")
print(f"DimDate rows: {dim_date.count()}")

# ── DimRegion ─────────────────────────────────────────────────────────────────

regions = [
    (1, "Région lémanique",      46.5197, 6.6323, "Geneva, Lausanne, Sion"),
    (2, "Espace Mittelland",     46.9480, 7.4474, "Bern, Fribourg, Neuchâtel"),
    (3, "Suisse du Nord-Ouest",  47.5596, 7.5886, "Basel, Aarau, Soleure"),
    (4, "Zurich",                47.3769, 8.5417, "Zurich, Winterthur"),
    (5, "Suisse orientale",      47.4245, 9.3767, "St-Gallen, Chur, Schaffhausen"),
    (6, "Suisse centrale",       47.0502, 8.3093, "Lucerne, Zug, Altdorf"),
    (7, "Tessin",                46.1984, 9.0237, "Lugano, Bellinzona, Locarno"),
]

schema = StructType([
    StructField("region_id",   IntegerType(), False),
    StructField("region_name", StringType(),  False),
    StructField("latitude",    DoubleType(),  False),
    StructField("longitude",   DoubleType(),  False),
    StructField("main_cities", StringType(),  False),
])

dim_region = spark.createDataFrame(regions, schema)
dim_region.write.mode("overwrite").format("delta").saveAsTable("dim_region")
print(f"DimRegion rows: {dim_region.count()}")

# ── FactUnemployment ──────────────────────────────────────────────────────────

dim_date   = spark.read.table("dim_date")
dim_region = spark.read.table("dim_region")

fact_unemp = df_unemp.join(dim_date.select("period", "date_id"), on="period", how="left")
fact_unemp = (fact_unemp
    .join(dim_region.select("region_id", "region_name"),
          fact_unemp["region"] == dim_region["region_name"], how="left")
    .drop("region_name"))

fact_unemp = fact_unemp.select(
    "date_id", "region_id", "unemployment_rate", "_run_id", "_ingested_at"
)

fact_unemp.write.mode("overwrite").format("delta").saveAsTable("fact_unemployment")
print(f"FactUnemployment rows: {fact_unemp.count()}")

# ── FactMacroIndicators ───────────────────────────────────────────────────────

df_cpi  = spark.read.table("silver_cpi")
df_rate = spark.read.table("silver_policy_rate")

df_cpi_pivot = (df_cpi
    .groupBy("period", "_run_id", "_ingested_at")
    .pivot("series_key")
    .agg({"cpi_value": "first"})
    .withColumnRenamed("LD2010100", "cpi_index")
    .withColumnRenamed("VVP", "cpi_yoy"))

fact_macro = df_cpi_pivot.join(
    df_rate.select("period", "swiss_policy_rate", "_rate_source"), on="period", how="left"
)
fact_macro = fact_macro.join(dim_date.select("period", "date_id"), on="period", how="left")

fact_macro = fact_macro.select(
    "date_id", "period", "cpi_index", "cpi_yoy",
    "swiss_policy_rate", "_rate_source", "_run_id", "_ingested_at"
).orderBy("period")

fact_macro.write.mode("overwrite").format("delta").saveAsTable("fact_macro_indicators")
print(f"FactMacroIndicators rows: {fact_macro.count()}")
