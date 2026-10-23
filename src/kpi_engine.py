"""
PySpark KPI Engine implementing the 3 Core Analytical Objectives
for the Air Quality Databricks Project.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, avg, max as sp_max, min as sp_min, count,
    round as sp_round, when, sum as sp_sum
)


class AirQualityKPIEngine:
    def __init__(self, df: DataFrame):
        self.df = df

    # =========================================================================
    # OBJECTIVE 1: Diurnal & Seasonal Temporal Pollution Dynamics (Rush-Hour Traffic)
    # =========================================================================
    def compute_objective1_rush_hour_metrics(self) -> DataFrame:
        """
        Quantifies emission surges during peak commuter rush hours vs non-rush hours.
        Calculates mean concentrations of primary combustion pollutants (CO, Benzene, NOx).
        """
        return self.df.groupBy("is_rush_hour").agg(
            sp_round(avg("co_gt"), 2).alias("avg_co_mg_m3"),
            sp_round(avg("c6h6_gt"), 2).alias("avg_benzene_ug_m3"),
            sp_round(avg("nox_gt"), 2).alias("avg_nox_ppb"),
            sp_round(avg("no2_gt"), 2).alias("avg_no2_ug_m3"),
            count("timestamp").alias("total_monitored_hours")
        ).orderBy("is_rush_hour")

    def compute_objective1_weekday_weekend_drop(self) -> DataFrame:
        """
        Evaluates the 'Weekend Effect': emission abatement when industrial & commuter traffic drops.
        """
        return self.df.groupBy("is_weekend").agg(
            sp_round(avg("co_gt"), 2).alias("avg_co"),
            sp_round(avg("c6h6_gt"), 2).alias("avg_benzene"),
            sp_round(avg("nox_gt"), 2).alias("avg_nox"),
            sp_round(avg("no2_gt"), 2).alias("avg_no2"),
            count("timestamp").alias("hourly_readings")
        ).orderBy("is_weekend")

    def compute_objective1_seasonal_diurnal_profile(self) -> DataFrame:
        """
        Computes 24-hour diurnal profile aggregated by season.
        """
        return self.df.groupBy("season", "hour").agg(
            sp_round(avg("co_gt"), 2).alias("mean_co"),
            sp_round(avg("c6h6_gt"), 2).alias("mean_benzene"),
            sp_round(avg("nox_gt"), 2).alias("mean_nox"),
            sp_round(avg("temperature"), 1).alias("mean_temp")
        ).orderBy("season", "hour")

    # =========================================================================
    # OBJECTIVE 2: Sensor Calibration & Multi-Sensor Cross-Correlation
    # =========================================================================
    def compute_objective2_sensor_correlation_matrix(self) -> dict:
        """
        Evaluates low-cost metal oxide chemical sensor responses against
        certified reference analyzer concentrations (Ground Truth).
        """
        correlations = {
            "PT08.S1(CO) vs CO(GT)": self.df.stat.corr("sensor_s1_co", "co_gt"),
            "PT08.S2(NMHC) vs C6H6(GT)": self.df.stat.corr("sensor_s2_nmhc", "c6h6_gt"),
            "PT08.S3(NOx) vs NOx(GT)": self.df.stat.corr("sensor_s3_nox", "nox_gt"),
            "PT08.S4(NO2) vs NO2(GT)": self.df.stat.corr("sensor_s4_no2", "no2_gt"),
            "PT08.S1(CO) vs Temperature": self.df.stat.corr("sensor_s1_co", "temperature"),
            "PT08.S1(CO) vs Rel_Humidity": self.df.stat.corr("sensor_s1_co", "relative_humidity"),
            "PT08.S2(NMHC) vs Temperature": self.df.stat.corr("sensor_s2_nmhc", "temperature"),
            "PT08.S2(NMHC) vs Rel_Humidity": self.df.stat.corr("sensor_s2_nmhc", "relative_humidity"),
        }
        return {k: round(v, 4) if v is not None else None for k, v in correlations.items()}

    def compute_objective2_temperature_binned_drift(self) -> DataFrame:
        """
        Analyzes how sensor reading accuracy varies across temperature bands
        (Thermal drift impact on metal-oxide resistance).
        """
        binned_df = self.df.withColumn(
            "temp_bracket",
            when(col("temperature") < 10.0, "< 10 C (Cold)")
            .when(col("temperature").between(10.0, 25.0), "10-25 C (Moderate)")
            .otherwise("> 25 C (Warm)")
        )

        return binned_df.groupBy("temp_bracket").agg(
            sp_round(avg("sensor_s1_co"), 1).alias("mean_sensor_s1"),
            sp_round(avg("co_gt"), 2).alias("mean_co_gt"),
            sp_round(avg("sensor_s2_nmhc"), 1).alias("mean_sensor_s2"),
            sp_round(avg("c6h6_gt"), 2).alias("mean_benzene_gt"),
            count("timestamp").alias("reading_count")
        ).orderBy("temp_bracket")

    # =========================================================================
    # OBJECTIVE 3: Environmental Health Compliance & Exceedance Threshold Monitoring
    # =========================================================================
    def compute_objective3_regulatory_summary(self) -> DataFrame:
        """
        Quantifies compliance against EU Directive 2008/50/EC limits:
        - NO2 hourly limit: 200 ug/m3
        - CO 8-hour maximum: 10 mg/m3
        - C6H6 high-risk exposure: > 10 ug/m3
        """
        return self.df.agg(
            count("no2_gt").alias("valid_no2_readings"),
            count(when(col("no2_gt") > 200.0, 1)).alias("no2_exceedance_hours"),
            sp_round(count(when(col("no2_gt") > 200.0, 1)) * 100.0 / count("no2_gt"), 2).alias("no2_exceedance_pct"),
            count("co_gt").alias("valid_co_readings"),
            count(when(col("co_gt") > 10.0, 1)).alias("co_severe_exceedance_hours"),
            sp_round(count(when(col("co_gt") > 10.0, 1)) * 100.0 / count("co_gt"), 3).alias("co_exceedance_pct"),
            count(when(col("c6h6_gt") > 10.0, 1)).alias("benzene_high_exposure_hours")
        )

    def compute_objective3_monthly_exceedances(self) -> DataFrame:
        """
        Monthly distribution of air quality alert violations to identify seasonal smog peaks.
        """
        return self.df.groupBy("year", "month").agg(
            count(when(col("eu_no2_exceedance") == 1, 1)).alias("no2_violations"),
            sp_round(count(when(col("eu_no2_exceedance") == 1, 1)) * 100.0 / count("no2_gt"), 2).alias("violation_rate_pct"),
            sp_round(avg("co_gt"), 2).alias("monthly_avg_co"),
            sp_round(avg("no2_gt"), 2).alias("monthly_avg_no2"),
            count("timestamp").alias("hours_in_month")
        ).orderBy("year", "month")

    def compute_objective3_rolling_8h_co(self) -> DataFrame:
        """
        Computes 8-hour rolling average Carbon Monoxide level using PySpark Window specifications.
        Required by EU / WHO air quality standards.
        """
        window_8h = Window.orderBy("timestamp").rowsBetween(-7, 0)
        return self.df.select(
            "timestamp", "co_gt", "temperature", "relative_humidity"
        ).withColumn(
            "co_8h_moving_avg",
            sp_round(avg("co_gt").over(window_8h), 2)
        ).withColumn(
            "co_8h_limit_exceeded",
            when(col("co_8h_moving_avg") > 10.0, 1).otherwise(0)
        )
