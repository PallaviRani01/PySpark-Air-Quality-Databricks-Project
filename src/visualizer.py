"""
Visualization module for the Air Quality PySpark Databricks project.
Generates publication-ready figures for the 3 objectives and KPI dashboards.
"""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, count, when, col, round as sp_round


class AirQualityVisualizer:
    def __init__(self, output_dir: str = "visualizations"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        # Apply standard clean styling
        sns.set_theme(style="whitegrid", palette="deep")
        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.size"] = 10

    def plot_diurnal_multi_pollutant(self, df: DataFrame) -> str:
        """
        Chart 1: 24-Hour Diurnal Curves for CO, Benzene, and NOx.
        Highlights morning and evening commuter rush hours.
        """
        hourly_pdf = df.groupBy("hour").agg(
            sp_round(avg("co_gt"), 2).alias("CO (mg/m³)"),
            sp_round(avg("c6h6_gt"), 2).alias("Benzene (µg/m³)"),
            sp_round(avg("nox_gt") / 20.0, 2).alias("NOx / 20 (ppb)")
        ).orderBy("hour").toPandas()

        fig, ax = plt.subplots(figsize=(11, 5.5))
        ax.plot(hourly_pdf["hour"], hourly_pdf["CO (mg/m³)"], marker="o", linewidth=2.5, color="#d62728", label="CO (mg/m³)")
        ax.plot(hourly_pdf["hour"], hourly_pdf["Benzene (µg/m³)"], marker="s", linewidth=2.5, color="#1f77b4", label="Benzene C6H6 (µg/m³)")
        ax.plot(hourly_pdf["hour"], hourly_pdf["NOx / 20 (ppb)"], marker="^", linewidth=2.5, color="#2ca02c", linestyle="--", label="NOx / 20 (ppb scaled)")

        # Highlight Rush Hours
        ax.axvspan(7.5, 9.5, color="#ffbb78", alpha=0.3, label="Morning Rush Hour (8-9 AM)")
        ax.axvspan(17.5, 20.5, color="#ff9896", alpha=0.3, label="Evening Rush Hour (6-8 PM)")

        ax.set_title("Objective 1: 24-Hour Diurnal Multi-Pollutant Dynamics & Commuter Peaks", fontsize=13, fontweight="bold", pad=12)
        ax.set_xlabel("Hour of Day (00:00 - 23:00)", fontweight="semibold")
        ax.set_ylabel("Average Concentration", fontweight="semibold")
        ax.set_xticks(range(0, 24))
        ax.legend(loc="upper left", frameon=True)
        ax.grid(True, linestyle=":", alpha=0.6)

        filepath = os.path.join(self.output_dir, "diurnal_multi_pollutant.png")
        plt.tight_layout()
        plt.savefig(filepath, dpi=200)
        plt.close()
        print(f"[OK] Saved visualization: {filepath}")
        return filepath

    def plot_weekend_vs_weekday(self, df: DataFrame) -> str:
        """
        Chart 2: Weekday vs Weekend comparative bar chart showing emission reduction.
        """
        pdf = df.groupBy("is_weekend").agg(
            sp_round(avg("co_gt"), 2).alias("co"),
            sp_round(avg("c6h6_gt"), 2).alias("benzene"),
            sp_round(avg("nox_gt"), 2).alias("nox"),
            sp_round(avg("no2_gt"), 2).alias("no2")
        ).toPandas()

        pdf["day_type"] = pdf["is_weekend"].map({0: "Weekday", 1: "Weekend"})

        melted = pd.melt(
            pdf,
            id_vars=["day_type"],
            value_vars=["co", "benzene", "no2"],
            var_name="Pollutant",
            value_name="Mean Concentration"
        )
        pollutant_labels = {"co": "CO (mg/m³)", "benzene": "Benzene (µg/m³)", "no2": "NO2 (µg/m³)"}
        melted["Pollutant"] = melted["Pollutant"].map(pollutant_labels)

        fig, ax = plt.subplots(figsize=(9, 5))
        sns.barplot(data=melted, x="Pollutant", y="Mean Concentration", hue="day_type", palette=["#2b5c8f", "#e26d5c"], ax=ax)

        ax.set_title("Objective 1: Traffic Abatement Impact (Weekday vs. Weekend Emissions)", fontsize=13, fontweight="bold", pad=12)
        ax.set_ylabel("Mean Concentration", fontweight="semibold")
        ax.set_xlabel("Pollutant Indicator", fontweight="semibold")
        ax.legend(title="Period Type", frameon=True)

        filepath = os.path.join(self.output_dir, "weekend_vs_weekday_comparison.png")
        plt.tight_layout()
        plt.savefig(filepath, dpi=200)
        plt.close()
        print(f"[OK] Saved visualization: {filepath}")
        return filepath

    def plot_sensor_calibration(self, df: DataFrame) -> str:
        """
        Chart 3: Low-Cost Sensor Calibration vs Reference Analyzers with Linear Trendlines.
        """
        sample_pdf = df.select("sensor_s2_nmhc", "c6h6_gt", "sensor_s1_co", "co_gt").dropna().sample(fraction=0.3, seed=42).toPandas()

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.5))

        # Sensor PT08.S2 vs Benzene
        sns.regplot(
            data=sample_pdf, x="sensor_s2_nmhc", y="c6h6_gt", ax=ax1,
            color="#1f77b4", scatter_kws={"alpha": 0.35, "s": 15}, line_kws={"color": "#d62728", "linewidth": 2}
        )
        r_benzene = sample_pdf["sensor_s2_nmhc"].corr(sample_pdf["c6h6_gt"])
        ax1.set_title(f"Sensor PT08.S2 vs. Benzene C6H6(GT)\nPearson Correlation r = {r_benzene:.3f} (High Fidelity)", fontsize=11, fontweight="bold")
        ax1.set_xlabel("PT08.S2 (Titania Sensor Response)")
        ax1.set_ylabel("True Benzene (µg/m³)")

        # Sensor PT08.S1 vs CO
        sns.regplot(
            data=sample_pdf, x="sensor_s1_co", y="co_gt", ax=ax2,
            color="#2ca02c", scatter_kws={"alpha": 0.35, "s": 15}, line_kws={"color": "#ff7f0e", "linewidth": 2}
        )
        r_co = sample_pdf["sensor_s1_co"].corr(sample_pdf["co_gt"])
        ax2.set_title(f"Sensor PT08.S1 vs. CO(GT)\nPearson Correlation r = {r_co:.3f} (Cross-Sensitivity Bias)", fontsize=11, fontweight="bold")
        ax2.set_xlabel("PT08.S1 (Tin Oxide Sensor Response)")
        ax2.set_ylabel("True Carbon Monoxide (mg/m³)")

        fig.suptitle("Objective 2: Low-Cost Chemical Sensor Calibration vs. Reference Analyzers", fontsize=14, fontweight="bold", y=1.02)
        filepath = os.path.join(self.output_dir, "sensor_calibration_correlation.png")
        plt.tight_layout()
        plt.savefig(filepath, dpi=200, bbox_inches="tight")
        plt.close()
        print(f"[OK] Saved visualization: {filepath}")
        return filepath

    def plot_weekly_pollution_heatmap(self, df: DataFrame) -> str:
        """
        Chart 4: Heatmap of Hourly Air Pollution Severity (NO2) across Day of Week vs Hour of Day.
        """
        heat_pdf = df.groupBy("day_of_week", "hour").agg(
            sp_round(avg("no2_gt"), 1).alias("avg_no2")
        ).toPandas()

        day_map = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
        heat_pdf["day_str"] = heat_pdf["day_of_week"].map(day_map)

        pivot_table = heat_pdf.pivot(index="day_str", columns="hour", values="avg_no2")
        ordered_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        pivot_table = pivot_table.reindex([d for d in ordered_days if d in pivot_table.index])

        fig, ax = plt.subplots(figsize=(12, 5))
        sns.heatmap(pivot_table, cmap="YlOrRd", annot=False, cbar_kws={"label": "Mean NO2 (µg/m³)"}, ax=ax)

        ax.set_title("Objective 3: Temporal Pollution Heatmap (Day of Week vs. Hour of Day)", fontsize=13, fontweight="bold", pad=12)
        ax.set_xlabel("Hour of the Day (0-23)", fontweight="semibold")
        ax.set_ylabel("Day of the Week", fontweight="semibold")

        filepath = os.path.join(self.output_dir, "hourly_weekday_pollution_heatmap.png")
        plt.tight_layout()
        plt.savefig(filepath, dpi=200)
        plt.close()
        print(f"[OK] Saved visualization: {filepath}")
        return filepath

    def plot_monthly_regulatory_exceedances(self, df: DataFrame) -> str:
        """
        Chart 5: Monthly EU NO2 Exceedance Violations (> 200 ug/m3).
        """
        monthly_pdf = df.groupBy("year", "month").agg(
            count(when(col("no2_gt") > 200.0, 1)).alias("violations"),
            sp_round(count(when(col("no2_gt") > 200.0, 1)) * 100.0 / count("no2_gt"), 2).alias("violation_pct")
        ).orderBy("year", "month").toPandas()

        monthly_pdf["year_month"] = monthly_pdf["year"].astype(str) + "-" + monthly_pdf["month"].astype(str).str.zfill(2)

        fig, ax1 = plt.subplots(figsize=(11, 5))
        color1 = "#20639b"
        color2 = "#ed553b"

        ax1.bar(monthly_pdf["year_month"], monthly_pdf["violations"], color=color1, alpha=0.85, width=0.55, label="Exceedance Hours")
        ax1.set_xlabel("Year-Month", fontweight="semibold")
        ax1.set_ylabel("Number of Violation Hours (> 200 µg/m³)", color=color1, fontweight="semibold")
        ax1.tick_params(axis="y", labelcolor=color1)
        ax1.set_xticklabels(monthly_pdf["year_month"], rotation=45, ha="right")

        ax2 = ax1.twinx()
        ax2.plot(monthly_pdf["year_month"], monthly_pdf["violation_pct"], color=color2, marker="o", linewidth=2.5, label="Violation Rate (%)")
        ax2.set_ylabel("Violation Rate (% of monitored hours)", color=color2, fontweight="semibold")
        ax2.tick_params(axis="y", labelcolor=color2)
        ax2.grid(False)

        fig.suptitle("Objective 3: EU Ambient Air Quality Limit Violations (Hourly NO2 > 200 µg/m³)", fontsize=13, fontweight="bold")
        filepath = os.path.join(self.output_dir, "regulatory_exceedances_monthly.png")
        plt.tight_layout()
        plt.savefig(filepath, dpi=200)
        plt.close()
        print(f"[OK] Saved visualization: {filepath}")
        return filepath
