# Fabric Notebook — Silver Transformation
# Run in Microsoft Fabric (PySpark kernel)
# Reads from SEM_LH/Files/bronze/, writes Delta tables to SEM_LH/Tables/

from functools import reduce
from pyspark.sql.functions import col, lit, regexp_replace, regexp_extract

# ── Silver Unemployment ───────────────────────────────────────────────────────

df = spark.read.parquet("Files/bronze/bronze_unemployment")
rejected = []

mask = col("FREQ") == "M"
rejected.append(df.filter(~mask).withColumn("_rejection_reason", lit("FREQ != M")))
df = df.filter(mask)

mask = col("PERIOD") >= "2010-M01"
rejected.append(df.filter(~mask).withColumn("_rejection_reason", lit("PERIOD < 2010-M01")))
df = df.filter(mask)

mask = col("GENDER_FR") == "Total"
rejected.append(df.filter(~mask).withColumn("_rejection_reason", lit("GENDER_FR != Total")))
df = df.filter(mask)

mask = col("INDICATORS_FR") == "NUTS2"
rejected.append(df.filter(~mask).withColumn("_rejection_reason", lit("INDICATORS_FR != NUTS2")))
df = df.filter(mask)

# Rename PERIOD before creating period to avoid case-insensitive conflict in Spark
df = df.withColumnRenamed("PERIOD", "period_raw")
df = df.withColumn("period", regexp_replace(col("period_raw"), "-M", "-"))

cols_to_drop = ["INDICATORS_HRCHY", "INDICATORS_DE", "GENDER_DE",
                "DETAILS_DE", "FREQ", "MEASURE_FR", "MEASURE_DE",
                "STATUS", "STATUS_1", "period_raw"]
df = df.drop(*cols_to_drop)
df = (df.withColumnRenamed("INDICATORS_FR", "indicator")
        .withColumnRenamed("GENDER_FR", "gender")
        .withColumnRenamed("DETAILS_FR", "region")
        .withColumnRenamed("VALUE", "unemployment_rate"))

df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_unemployment")
df_rejected = reduce(lambda a, b: a.union(b), rejected)
df_rejected.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_unemployment_rejected")

print(f"[unemployment] accepted: {df.count()} | rejected: {df_rejected.count()}")

# ── Silver CPI ────────────────────────────────────────────────────────────────

df = spark.read.parquet("Files/bronze/bronze_cpi")
rejected = []

mask = col("date") >= "2010-01"
rejected.append(df.filter(~mask).withColumn("_rejection_reason", lit("date < 2010-01")))
df = df.filter(mask)

df = df.withColumn("series_key", regexp_extract(col("_series_key"), r'\{(\w+)\}', 1))
df = (df.withColumnRenamed("date", "period")
        .withColumnRenamed("value", "cpi_value")
        .drop("_series_key", "_series_name"))

df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_cpi")
df_rejected = reduce(lambda a, b: a.union(b), rejected)
df_rejected.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_cpi_rejected")

print(f"[cpi] accepted: {df.count()} | rejected: {df_rejected.count()}")

# ── Silver Policy Rate ────────────────────────────────────────────────────────

df = spark.read.parquet("Files/bronze/bronze_policy_rate")
rejected = []

swiss_mask = col("_series_name").startswith("Suisse")
rejected.append(df.filter(~swiss_mask).withColumn("_rejection_reason", lit("Non-Swiss central bank")))
df_swiss = df.filter(swiss_mask)

df_rate  = df_swiss.filter(col("_series_name").contains("Taux directeur de la BNS"))
df_lower = df_swiss.filter(col("_series_name").contains("limite inférieure"))
df_upper = df_swiss.filter(col("_series_name").contains("limite supérieure"))

# LIBOR midpoint 2010-01 → 2019-05
df_libor = (df_lower
    .join(df_upper.select(col("date"), col("value").alias("value_upper")), on="date")
    .withColumn("swiss_policy_rate", (col("value") + col("value_upper")) / 2)
    .filter((col("date") >= "2010-01") & (col("date") < "2019-06"))
    .select("date", "swiss_policy_rate", "_run_id", "_ingested_at", "_source", "_source_url")
    .withColumn("_rate_source", lit("LIBOR_midpoint")))

# SNB policy rate 2019-06 → present
df_snb = (df_rate
    .filter(col("date") >= "2019-06")
    .withColumnRenamed("value", "swiss_policy_rate")
    .select("date", "swiss_policy_rate", "_run_id", "_ingested_at", "_source", "_source_url")
    .withColumn("_rate_source", lit("SNB_policy_rate")))

df_final = df_libor.union(df_snb).withColumnRenamed("date", "period").orderBy("period")

df_final.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_policy_rate")
df_rejected = reduce(lambda a, b: a.union(b), rejected)
df_rejected.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_policy_rate_rejected")

print(f"[policy_rate] accepted: {df_final.count()} | rejected: {df_rejected.count()}")
