"""
Air Quality PySpark & Databricks Analytics Project
Standalone End-to-End Pipeline Runner
"""

import os
import sys
import time

# Guard against local SPARK_HOME version mismatch on local Windows installations
os.environ.pop("SPARK_HOME", None)

from pyspark.sql import SparkSession
from src.etl_pipeline import AirQualityETL
from src.kpi_engine import AirQualityKPIEngine
from src.visualizer import AirQualityVisualizer


def create_spark_session(app_name: str = "AirQualityAnalytics") -> SparkSession:
    """Builds and tunes the local SparkSession."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    start_time = time.time()
    print("=" * 78)
    print(" AIR QUALITY PYSPARK & DATABRICKS ANALYTICS PIPELINE")
    print(" Dataset: UCI Air Quality Multisensor Array (Italian Urban Field Test)")
    print("=" * 78)

    # 1. Initialize Spark
    print("\n[Stage 1] Initializing PySpark Session...")
    spark = create_spark_session()
    print(f"[OK] Spark Engine active (Version: {spark.version})")

    # Locate dataset
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_dir, "data", "AirQualityUCI.csv")
    if not os.path.exists(data_path):
        data_path = os.path.join("data", "AirQualityUCI.csv")

    # 2. Run ETL Pipeline (Timestamps [4:12] and [6:17])
    print("\n[Stage 2] [4:12] Dataset Load & [6:17] PySpark DataFrame Transformation...")
    etl = AirQualityETL(spark)
    df = etl.run(data_path)
    total_records = df.count()
    print(f"[OK] Successfully ingested & curated {total_records:,} hourly environmental records.")

    print("\nCurated DataFrame Sample:")
    df.select("timestamp", "co_gt", "c6h6_gt", "nox_gt", "sensor_s1_co", "temperature", "is_rush_hour").show(5)

    # Register temporary SQL view for Databricks SQL queries
    df.createOrReplaceTempView("air_quality_curated")
    print("[OK] Temporary view 'air_quality_curated' registered for Spark SQL.")

    # 3. KPI Development & Implementation (Timestamp [18:55])
    print("\n[Stage 3] [18:55] Executing 3 Analytical Objectives & KPI Engine...")
    kpi_engine = AirQualityKPIEngine(df)

    # --- OBJECTIVE 1 ---
    print("\n>>> OBJECTIVE 1: Diurnal & Rush-Hour Traffic Emission Dynamics")
    rush_hour_df = kpi_engine.compute_objective1_rush_hour_metrics()
    rush_hour_df.show()

    weekend_df = kpi_engine.compute_objective1_weekday_weekend_drop()
    print("Weekday (0) vs. Weekend (1) Traffic Abatement:")
    weekend_df.show()

    # --- OBJECTIVE 2 ---
    print("\n>>> OBJECTIVE 2: Sensor Calibration & Environmental Interference Analysis")
    corr_dict = kpi_engine.compute_objective2_sensor_correlation_matrix()
    print("Cross-Correlation Matrix (Low-Cost Sensor vs Ground Truth Analyzers):")
    for pair, val in corr_dict.items():
        print(f"  * {pair:<32}: {val:>7.4f}")

    print("\nSensor Thermal Drift across Temperature Brackets:")
    temp_drift_df = kpi_engine.compute_objective2_temperature_binned_drift()
    temp_drift_df.show()

    # --- OBJECTIVE 3 ---
    print("\n>>> OBJECTIVE 3: Environmental Health Compliance & EU Exceedance Monitoring")
    regulatory_summary = kpi_engine.compute_objective3_regulatory_summary()
    regulatory_summary.show()

    print("Monthly Breakdown of EU Limit Exceedance Hours:")
    monthly_exceedances = kpi_engine.compute_objective3_monthly_exceedances()
    monthly_exceedances.show(12)

    # 4. Visualizations & Dashboards (Timestamp [41:15])
    print("\n[Stage 4] [41:15] Generating Dashboard Visualizations...")
    viz_dir = os.path.join(base_dir, "visualizations")
    viz = AirQualityVisualizer(output_dir=viz_dir)

    viz.plot_diurnal_multi_pollutant(df)
    viz.plot_weekend_vs_weekday(df)
    viz.plot_sensor_calibration(df)
    viz.plot_weekly_pollution_heatmap(df)
    viz.plot_monthly_regulatory_exceedances(df)

    spark.stop()
    elapsed = time.time() - start_time
    print("\n" + "=" * 78)
    print(f" [OK] Pipeline successfully finished in {elapsed:.2f} seconds!")
    print(f" [OK] Visualizations stored in: {viz_dir}")
    print("=" * 78)


if __name__ == "__main__":
    main()
