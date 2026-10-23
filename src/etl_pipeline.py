"""
PySpark ETL Pipeline for UCI Air Quality Dataset.
Handles European CSV delimiters, comma decimals, missing sentinel values (-200),
and engineers temporal & air quality hazard features.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, to_timestamp, concat, lit, regexp_replace,
    hour, dayofmonth, month, year, dayofweek, date_format
)
from src.config import (
    ORDERED_CLEAN_NAMES, DECIMAL_COLUMNS, INTEGER_COLUMNS,
    ALL_METRIC_COLUMNS, MISSING_VALUE_SENTINEL, ALL_RUSH_HOURS
)


class AirQualityETL:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_raw_data(self, file_path: str) -> DataFrame:
        """
        Loads the raw UCI CSV dataset with semicolon delimiter.
        Positional mapping is used to avoid period-in-column-name parsing issues.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Dataset not found at: {file_path}")

        raw_df = (
            self.spark.read
            .option("header", "true")
            .option("delimiter", ";")
            .csv(file_path)
        )

        # Map the first 15 columns to clean names and drop phantom trailing columns
        total_cols = len(raw_df.columns)
        dummy_names = [f"c_{i}" for i in range(total_cols)]
        for i, clean_name in enumerate(ORDERED_CLEAN_NAMES):
            if i < total_cols:
                dummy_names[i] = clean_name

        df_mapped = raw_df.toDF(*dummy_names)

        # Retain only the 15 valid attributes
        df_retained = df_mapped.select(*ORDERED_CLEAN_NAMES)
        return df_retained

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans the DataFrame:
        - Filters out trailing empty rows where Date is null/blank
        - Replaces European comma decimal separators with dots
        - Casts columns to DoubleType
        - Replaces -200 sentinel values with Spark NULLs
        """
        # Filter valid records
        df_valid = df.filter(col("date_str").isNotNull() & (col("date_str") != ""))

        # Replace European comma decimals (e.g., '2,6' -> '2.6') and cast to double
        for c in DECIMAL_COLUMNS:
            df_valid = df_valid.withColumn(c, regexp_replace(col(c), ",", ".").cast("double"))

        # Cast integer sensors
        for c in INTEGER_COLUMNS:
            df_valid = df_valid.withColumn(c, col(c).cast("double"))

        # Replace -200 sentinel values with NULL
        for c in ALL_METRIC_COLUMNS:
            df_valid = df_valid.withColumn(
                c, when(col(c) == MISSING_VALUE_SENTINEL, None).otherwise(col(c))
            )

        return df_valid

    def engineer_features(self, df: DataFrame) -> DataFrame:
        """
        Engineers temporal and domain-specific air pollution features:
        - Timestamp (parsed from dd/MM/yyyy and HH.mm.ss)
        - Hour, Day, Month, Year, Day of Week, Day Name
        - Is Weekend indicator
        - Peak Rush Hour indicator (08:00-09:59 & 18:00-20:59)
        - Season classification (Winter, Spring, Summer, Autumn)
        - Composite Health Risk Alert Flag (EU NO2 > 200 ug/m3 or CO > 10 mg/m3)
        """
        # Synthesize combined timestamp
        df_features = df.withColumn(
            "timestamp",
            to_timestamp(concat(col("date_str"), lit(" "), col("time_str")), "dd/MM/yyyy HH.mm.ss")
        )

        # Date-time features
        df_features = (
            df_features
            .withColumn("hour", hour(col("timestamp")))
            .withColumn("day", dayofmonth(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("year", year(col("timestamp")))
            .withColumn("day_of_week", dayofweek(col("timestamp")))
            .withColumn("day_name", date_format(col("timestamp"), "EEEE"))
            .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0))
            .withColumn("is_rush_hour", when(col("hour").isin(ALL_RUSH_HOURS), 1).otherwise(0))
        )

        # Meteorological Season
        df_features = df_features.withColumn(
            "season",
            when(col("month").isin(12, 1, 2), "Winter")
            .when(col("month").isin(3, 4, 5), "Spring")
            .when(col("month").isin(6, 7, 8), "Summer")
            .otherwise("Autumn")
        )

        # Time of Day period
        df_features = df_features.withColumn(
            "time_period",
            when(col("hour").between(7, 9), "Morning Rush")
            .when(col("hour").between(10, 16), "Daytime Off-Peak")
            .when(col("hour").between(17, 20), "Evening Rush")
            .otherwise("Night Off-Peak")
        )

        # EU Regulatory Alert Flags
        df_features = df_features.withColumn(
            "eu_no2_exceedance",
            when(col("no2_gt") > 200.0, 1).otherwise(0)
        ).withColumn(
            "eu_co_exceedance",
            when(col("co_gt") > 10.0, 1).otherwise(0)
        ).withColumn(
            "air_quality_alert",
            when((col("no2_gt") > 200.0) | (col("co_gt") > 10.0), 1).otherwise(0)
        )

        return df_features

    def run(self, file_path: str) -> DataFrame:
        """Executes the complete ETL pipeline returning a curated PySpark DataFrame."""
        print(f"[*] Ingesting raw dataset from: {file_path}")
        raw_df = self.load_raw_data(file_path)

        print(f"[*] Cleaning missing values (-200) and standardizing schema...")
        cleaned_df = self.clean_data(raw_df)

        print(f"[*] Engineering temporal, traffic, and regulatory features...")
        final_df = self.engineer_features(cleaned_df)

        return final_df
