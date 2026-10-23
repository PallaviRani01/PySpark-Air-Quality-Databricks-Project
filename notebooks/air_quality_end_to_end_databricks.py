# Databricks notebook source
# MAGIC %md
# MAGIC # Urban Air Quality Analytics & KPI Engine (UCI Dataset #360)
# MAGIC **Dataset Reference:** [UCI Machine Learning Repository - Air Quality Dataset](https://archive.ics.uci.edu/dataset/360/air+quality)
# MAGIC
# MAGIC ### Project Milestones:
# MAGIC - **[4:12] Dataset Load:** Ingesting raw CSV via DBFS / Unity Catalog Volumes with European semicolon delimiter (`;`)
# MAGIC - **[6:17] PySpark DataFrame:** Missing value cleaning (`-200` to NULL), decimal parsing, timestamp creation, and calendar features
# MAGIC - **[18:55] Project Implementation & KPI Development:**
# MAGIC   - **Objective 1:** Diurnal Rush-Hour Patterns & Weekend Traffic Abatement
# MAGIC   - **Objective 2:** Sensor Calibration & Multi-Sensor Cross-Correlation
# MAGIC   - **Objective 3:** Environmental Health Compliance & EU Exceedance Monitoring
# MAGIC - **[41:15] Dashboard Visualization:** Databricks Lakeview / SQL Dashboard widgets & inline visual charts

# COMMAND ----------
# MAGIC %md
# MAGIC ## [4:12] Stage 1: Dataset Load & Schema Mapping

# COMMAND ----------
from pyspark.sql.functions import (
    col, when, to_timestamp, concat, lit, regexp_replace,
    hour, dayofweek, month, year, avg, count, round as sp_round, sum as sp_sum
)
from pyspark.sql import Window

# Databricks file path (e.g. /FileStore/tables/AirQualityUCI.csv or Unity Catalog Volume)
data_path = "dbfs:/FileStore/tables/AirQualityUCI.csv"

# For local execution fallback:
import os
if not data_path.startswith("dbfs:") or not os.path.exists(data_path):
    data_path = "../data/AirQualityUCI.csv"

raw_df = spark.read.option("header", "true").option("delimiter", ";").csv(data_path)

clean_columns = [
    "date_str", "time_str", "co_gt", "sensor_s1_co", "nmhc_gt", "c6h6_gt",
    "sensor_s2_nmhc", "nox_gt", "sensor_s3_nox", "no2_gt", "sensor_s4_no2",
    "sensor_s5_o3", "temperature", "relative_humidity", "absolute_humidity"
]

# Avoid dot-in-column-name parsing errors by positionally mapping the 15 columns
dummy_names = [f"c_{i}" for i in range(len(raw_df.columns))]
for i, name in enumerate(clean_columns):
    dummy_names[i] = name

df_mapped = raw_df.toDF(*dummy_names).select(*clean_columns)
display(df_mapped.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## [6:17] Stage 2: PySpark DataFrame Cleaning & Feature Engineering

# COMMAND ----------
# 1. Filter out empty rows
df_valid = df_mapped.filter(col("date_str").isNotNull() & (col("date_str") != ""))

# 2. Convert European comma decimals to dots and cast to DoubleType
decimal_cols = ["co_gt", "c6h6_gt", "temperature", "relative_humidity", "absolute_humidity"]
for c in decimal_cols:
    df_valid = df_valid.withColumn(c, regexp_replace(col(c), ",", ".").cast("double"))

# 3. Cast sensor integer fields
sensor_cols = ["sensor_s1_co", "nmhc_gt", "sensor_s2_nmhc", "nox_gt", "sensor_s3_nox", "no2_gt", "sensor_s4_no2", "sensor_s5_o3"]
for c in sensor_cols:
    df_valid = df_valid.withColumn(c, col(c).cast("double"))

# 4. Replace -200 sentinel values with Spark NULLs
for c in decimal_cols + sensor_cols:
    df_valid = df_valid.withColumn(c, when(col(c) == -200, None).otherwise(col(c)))

# 5. Synthesize unified Timestamp and extract features
df_curated = (
    df_valid
    .withColumn("timestamp", to_timestamp(concat(col("date_str"), lit(" "), col("time_str")), "dd/MM/yyyy HH.mm.ss"))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", dayofweek(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("year", year(col("timestamp")))
    .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0))
    .withColumn("is_rush_hour", when(col("hour").isin(8, 9, 18, 19, 20), 1).otherwise(0))
    .withColumn(
        "season",
        when(col("month").isin(12, 1, 2), "Winter")
        .when(col("month").isin(3, 4, 5), "Spring")
        .when(col("month").isin(6, 7, 8), "Summer")
        .otherwise("Autumn")
    )
)

# Register as Spark SQL temporary view and save as Delta Lake Table
df_curated.createOrReplaceTempView("air_quality_curated")
# df_curated.write.format("delta").mode("overwrite").saveAsTable("air_quality_curated_delta")

print(f"Total curated records: {df_curated.count():,}")
display(df_curated.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## [18:55] Stage 3: Project Implementation & KPI Development
# MAGIC ### Objective 1: Diurnal Rush-Hour Patterns & Weekend Traffic Abatement

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Objective 1: Rush Hour vs Off-Peak Pollution Comparison
# MAGIC SELECT 
# MAGIC     CASE WHEN is_rush_hour = 1 THEN 'Peak Rush Hour (8-9 AM, 6-8 PM)' ELSE 'Off-Peak Hours' END AS traffic_period,
# MAGIC     ROUND(AVG(co_gt), 2) AS avg_co_mg_m3,
# MAGIC     ROUND(AVG(c6h6_gt), 2) AS avg_benzene_ug_m3,
# MAGIC     ROUND(AVG(nox_gt), 2) AS avg_nox_ppb,
# MAGIC     ROUND(AVG(no2_gt), 2) AS avg_no2_ug_m3,
# MAGIC     COUNT(*) AS total_hours
# MAGIC FROM air_quality_curated
# MAGIC GROUP BY is_rush_hour
# MAGIC ORDER BY is_rush_hour DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Objective 1: Weekend Effect (Traffic Abatement Drop %)
# MAGIC SELECT 
# MAGIC     CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS period,
# MAGIC     ROUND(AVG(co_gt), 2) AS avg_co,
# MAGIC     ROUND(AVG(c6h6_gt), 2) AS avg_benzene,
# MAGIC     ROUND(AVG(nox_gt), 2) AS avg_nox,
# MAGIC     ROUND(AVG(no2_gt), 2) AS avg_no2
# MAGIC FROM air_quality_curated
# MAGIC GROUP BY is_weekend;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Objective 2: Sensor Calibration & Cross-Correlation Analysis

# COMMAND ----------
# Objective 2: Compute Pearson Correlation Matrix
r_co = df_curated.stat.corr("sensor_s1_co", "co_gt")
r_benzene = df_curated.stat.corr("sensor_s2_nmhc", "c6h6_gt")
r_nox = df_curated.stat.corr("sensor_s3_nox", "nox_gt")
r_no2 = df_curated.stat.corr("sensor_s4_no2", "no2_gt")

print(f"Correlation PT08.S1(CO) vs CO(GT):       r = {r_co:.4f}")
print(f"Correlation PT08.S2(NMHC) vs C6H6(GT):   r = {r_benzene:.4f} (High Accuracy)")
print(f"Correlation PT08.S3(NOx) vs NOx(GT):     r = {r_nox:.4f} (Inverted Resistance)")
print(f"Correlation PT08.S4(NO2) vs NO2(GT):     r = {r_no2:.4f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Objective 3: Environmental Health Compliance & EU Limit Violations

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Objective 3: European Union Limit Exceedance Summary (EU Directive 2008/50/EC)
# MAGIC SELECT 
# MAGIC     COUNT(no2_gt) AS total_valid_hours,
# MAGIC     SUM(CASE WHEN no2_gt > 200.0 THEN 1 ELSE 0 END) AS no2_exceedance_hours,
# MAGIC     ROUND(SUM(CASE WHEN no2_gt > 200.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(no2_gt), 2) AS no2_exceedance_pct,
# MAGIC     SUM(CASE WHEN co_gt > 10.0 THEN 1 ELSE 0 END) AS co_severe_spikes
# MAGIC FROM air_quality_curated;

# COMMAND ----------
# MAGIC %md
# MAGIC ## [41:15] Stage 4: Dashboard Visualization & Visual Reporting

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 24-Hour Diurnal Curves for Databricks Lakeview Visualizer
# MAGIC SELECT 
# MAGIC     hour,
# MAGIC     ROUND(AVG(co_gt), 2) AS avg_co,
# MAGIC     ROUND(AVG(c6h6_gt), 2) AS avg_benzene,
# MAGIC     ROUND(AVG(nox_gt) / 20.0, 2) AS avg_nox_scaled
# MAGIC FROM air_quality_curated
# MAGIC WHERE co_gt IS NOT NULL
# MAGIC GROUP BY hour
# MAGIC ORDER BY hour;
