# 🌍 Urban Air Quality Analytics & KPI Engine
### End-to-End PySpark & Databricks Lakehouse Analytics Project

[![PySpark](https://img.shields.io/badge/PySpark-4.2.0-orange.svg)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-blue.svg)](https://www.databricks.com/)
[![UCI Dataset](https://img.shields.io/badge/UCI-Air%20Quality%20%23360-green.svg)](https://archive.ics.uci.edu/dataset/360/air+quality)
[![Environment](https://img.shields.io/badge/Execution-Windows%20CMD-black.svg)]()
[![Visualizations](https://img.shields.io/badge/Visuals-VS%20Code-purple.svg)]()

---

## 📌 Project Overview
This project builds an enterprise-grade Big Data pipeline and analytics engine on the **UCI Air Quality Multisensor Array Dataset** (recorded at road-level in a polluted Italian city over an entire year: March 2004 – February 2005).

The project is structured according to the classic data engineering and analytics portfolio milestones:
- **`[4:12]` Dataset Load:** Ingesting raw CSV with European delimiters (`;`), comma decimals, and robust schema mapping.
- **`[6:17]` PySpark DataFrame:** Handling missing values tagged as `-200`, casting types, synthesizing timestamps, and engineering temporal/traffic features.
- **`[18:55]` Project Implementation & KPI Development:** Implementing **3 Core Business & Environmental Objectives**.
- **`[41:15]` Dashboard Visualization:** Publication-ready charts, VS Code interactive dashboard, and Databricks SQL queries.

---

## 🎯 The 3 Core Analytical Objectives & KPIs

### **Objective 1: Diurnal & Seasonal Temporal Pollution Analysis (Rush-Hour Traffic)**
* **Context:** Quantifies the impact of urban commuter cycles on primary combustion pollutants ($CO$, Benzene $C_6H_6$, and Nitrogen Oxides $NO_x$).
* **Key Findings & KPIs:**
  - **Rush-Hour Surge Ratio:** Carbon Monoxide averages **3.29 mg/m³** during peak rush hours (08:00–09:59 & 18:00–20:59) vs. **1.84 mg/m³** during off-peak hours (**+78.8% surge**). Benzene surges **+88.9%** (16.08 µg/m³ vs 8.51 µg/m³).
  - **Weekend Abatement Effect:** Weekends show a **30.1% drop** in $CO$ (2.36 to 1.65 mg/m³) and a **35.3% drop** in Benzene (11.25 to 7.28 µg/m³).

### **Objective 2: Sensor Calibration & Environmental Drift Analysis**
* **Context:** Evaluates low-cost chemical metal-oxide sensors (`PT08.S1` to `PT08.S5`) against certified Reference Ground Truth Analyzers.
* **Key Findings & KPIs:**
  - **Highest Fidelity Sensor:** **PT08.S2 (Titania sensor)** exhibits an exceptional Pearson correlation of **$r = 0.9262$** with true Benzene $C_6H_6(GT)$, making it viable as a reliable low-cost monitor.
  - **Cross-Sensitivity & Interference:** **PT08.S1 (Tin Oxide)** exhibits cross-sensitivity with Relative Humidity ($r = 0.4175$) and Temperature ($r = 0.3004$), requiring humidity compensation algorithms.
  - **Thermal Drift:** In cold temperatures ($< 10^\circ C$), mean sensor response drops to 1,054.5 compared to 1,114.9 at moderate temperatures ($10–25^\circ C$).

### **Objective 3: Environmental Health Compliance & EU Exceedance Monitoring**
* **Context:** Tracks compliance against European Union Ambient Air Quality Standards (**EU Directive 2008/50/EC**).
* **Key Findings & KPIs:**
  - **EU $NO_2$ Hourly Limit ($200\ \mu g/m^3$):** Total of **386 violation hours** (**5.00%** of all valid monitored hours).
  - **EU $CO$ 8-Hour Maximum Standard ($10\ mg/m^3$):** 5 severe episodic limit exceedance spikes (**0.065%** violation rate).
  - **Seasonal Risk Inversion:** Winter months suffer extreme air quality alert spikes, peaking in February 2005 with **160 violation hours (24.92% of the entire month)**.

---

## 📂 Project Directory Structure

```
air_quality_databricks_pyspark/
│
├── run_project.bat                            <-- [CMD] 1-Click Windows Command Prompt Runner
├── run_pipeline.py                            <-- Main Python/PySpark execution pipeline
├── requirements.txt                           <-- Python package requirements
├── README.md                                  <-- Complete documentation
│
├── data/
│   ├── AirQualityUCI.csv                      <-- Raw UCI Air Quality Dataset (semicolon delimiter)
│   └── AirQualityUCI.xlsx                     <-- Excel version
│
├── notebooks/
│   ├── air_quality_vscode_dashboard.ipynb     <-- [VS CODE] Interactive Jupyter Notebook
│   └── air_quality_end_to_end_databricks.py   <-- [DATABRICKS] Ready-to-import notebook (%md, %python, %sql)
│
├── src/
│   ├── __init__.py
│   ├── config.py                              <-- Schema definitions, thresholds, sensor mappings
│   ├── etl_pipeline.py                        <-- PySpark ETL class (cleans -200, handles comma decimals)
│   ├── kpi_engine.py                          <-- Implementation of the 3 Objectives & KPI metrics
│   └── visualizer.py                          <-- Publication-quality chart generator
│
├── sql/
│   └── databricks_kpi_dashboard_queries.sql   <-- Databricks Lakeview / SQL Dashboard queries
│
└── visualizations/                            <-- [VS CODE] Generated dashboard figures & preview
    ├── dashboard.html                         <-- Interactive Executive HTML Dashboard
    ├── diurnal_multi_pollutant.png            <-- 24-hour diurnal curves with rush-hour shading
    ├── weekend_vs_weekday_comparison.png      <-- Weekend effect traffic abatement bar chart
    ├── sensor_calibration_correlation.png     <-- Low-cost sensor vs Ground truth regression scatter
    ├── hourly_weekday_pollution_heatmap.png   <-- Day of Week vs Hour of Day pollution heatmap
    └── regulatory_exceedances_monthly.png     <-- Monthly EU threshold exceedance violations
```

---

## 💻 1. Running the Project via Windows Command Prompt (CMD)

As requested, all pipeline execution is configured for standard Windows **Command Prompt (`cmd.exe`)**:

### Step 1: Open Windows Command Prompt
Press `Win + R`, type `cmd`, and press `Enter`.

### Step 2: Navigate to the Project Directory
```cmd
cd /d "d:\dsa project\air_quality_databricks_pyspark"
```

### Step 3: Run the Pipeline via Batch File
```cmd
run_project.bat
```
*Alternatively, you can run directly with python:*
```cmd
python run_pipeline.py
```

### Execution Output:
```
==============================================================================
 AIR QUALITY PYSPARK & DATABRICKS ANALYTICS PIPELINE
 Dataset: UCI Air Quality Multisensor Array (Italian Urban Field Test)
==============================================================================

[Stage 1] Initializing PySpark Session...
[OK] Spark Engine active (Version: 4.2.0)

[Stage 2] [4:12] Dataset Load & [6:17] PySpark DataFrame Transformation...
[*] Ingesting raw dataset from: data\AirQualityUCI.csv
[*] Cleaning missing values (-200) and standardizing schema...
[*] Engineering temporal, traffic, and regulatory features...
[OK] Successfully ingested & curated 9,357 hourly environmental records.

[Stage 3] [18:55] Executing 3 Analytical Objectives & KPI Engine...
>>> OBJECTIVE 1: Diurnal & Rush-Hour Traffic Emission Dynamics
>>> OBJECTIVE 2: Sensor Calibration & Environmental Interference Analysis
>>> OBJECTIVE 3: Environmental Health Compliance & EU Exceedance Monitoring

[Stage 4] [41:15] Generating Dashboard Visualizations...
[OK] Saved visualization: visualizations\diurnal_multi_pollutant.png
[OK] Saved visualization: visualizations\weekend_vs_weekday_comparison.png
[OK] Saved visualization: visualizations\sensor_calibration_correlation.png
[OK] Saved visualization: visualizations\hourly_weekday_pollution_heatmap.png
[OK] Saved visualization: visualizations\regulatory_exceedances_monthly.png

==============================================================================
 [OK] Pipeline successfully finished in 16.77 seconds!
 [OK] Visualizations stored in: visualizations
==============================================================================
```

---

## 🎨 2. Visualizing Inside VS Code

All visualization assets are tailored specifically for **VS Code**:

### **Option A: Interactive VS Code Jupyter Notebook**
1. In VS Code, open the file:
   [`notebooks/air_quality_vscode_dashboard.ipynb`](notebooks/air_quality_vscode_dashboard.ipynb)
2. Select your Python kernel in the top-right corner.
3. Click **"Run All"** to interactively step through the data cleaning, KPI tables, and view the high-resolution Seaborn/Matplotlib plots rendered directly inside the VS Code editor!

### **Option B: View Generated High-Resolution Charts**
You can click any image file inside the [`visualizations/`](visualizations/) folder in VS Code's file explorer:
- [`diurnal_multi_pollutant.png`](visualizations/diurnal_multi_pollutant.png) — Diurnal curves showing morning and evening traffic spikes.
- [`weekend_vs_weekday_comparison.png`](visualizations/weekend_vs_weekday_comparison.png) — Bar chart of weekend traffic abatement.
- [`sensor_calibration_correlation.png`](visualizations/sensor_calibration_correlation.png) — Regression scatter plots comparing sensors against reference analyzers.
- [`hourly_weekday_pollution_heatmap.png`](visualizations/hourly_weekday_pollution_heatmap.png) — Matrix heatmap of NO2 pollution across 7 days × 24 hours.
- [`regulatory_exceedances_monthly.png`](visualizations/regulatory_exceedances_monthly.png) — Monthly EU threshold violations.

### **Option C: Interactive Executive Dashboard (`dashboard.html`)**
1. Right-click on [`visualizations/dashboard.html`](visualizations/dashboard.html) in the VS Code file explorer.
2. Select **"Show Preview"** (or use the VS Code Simple Browser extension / Live Server).
3. Experience the full executive dashboard with interactive KPI cards, structured analytical tabs, and Databricks SQL snippets.

---

## ☁️ 3. Deploying to Databricks Workspace
If you also want to run this in a Databricks cluster:
1. In your Databricks workspace, go to **Workspace -> Import**.
2. Select [`notebooks/air_quality_end_to_end_databricks.py`](notebooks/air_quality_end_to_end_databricks.py).
3. Upload `data/AirQualityUCI.csv` to DBFS or Unity Catalog Volumes.
4. Attach a cluster (Runtime 13.x+ with Spark 3.5 or 4.x) and click **"Run All"**.
5. Use the queries in [`sql/databricks_kpi_dashboard_queries.sql`](sql/databricks_kpi_dashboard_queries.sql) to build a Databricks Lakeview Dashboard!
