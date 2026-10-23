-- =============================================================================
-- DATABRICKS SQL DASHBOARD & LAKEVIEW QUERIES
-- Project: Urban Air Quality Analytics (UCI Dataset #360)
-- Target Table: air_quality_curated (Delta Table / Spark SQL View)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- OBJECTIVE 1: Diurnal & Rush-Hour Traffic Emission Dynamics
-- -----------------------------------------------------------------------------

-- Widget 1.1: 24-Hour Diurnal Multi-Pollutant Curve
SELECT 
    hour,
    ROUND(AVG(co_gt), 2) AS mean_co_mg_m3,
    ROUND(AVG(c6h6_gt), 2) AS mean_benzene_ug_m3,
    ROUND(AVG(nox_gt), 2) AS mean_nox_ppb,
    ROUND(AVG(no2_gt), 2) AS mean_no2_ug_m3,
    CASE 
        WHEN hour IN (8, 9, 18, 19, 20) THEN 'Commuter Peak Rush' 
        ELSE 'Off-Peak Base' 
    END AS traffic_class
FROM air_quality_curated
WHERE co_gt IS NOT NULL
GROUP BY hour
ORDER BY hour ASC;

-- Widget 1.2: Traffic Abatement (Weekday vs. Weekend Effect)
SELECT 
    CASE WHEN is_weekend = 1 THEN 'Weekend (Low Traffic)' ELSE 'Weekday (High Traffic)' END AS day_type,
    ROUND(AVG(co_gt), 2) AS avg_co,
    ROUND(AVG(c6h6_gt), 2) AS avg_benzene,
    ROUND(AVG(nox_gt), 2) AS avg_nox,
    ROUND(AVG(no2_gt), 2) AS avg_no2,
    COUNT(*) AS total_hours_monitored
FROM air_quality_curated
GROUP BY is_weekend
ORDER BY is_weekend ASC;

-- Widget 1.3: Seasonal Variation of Primary Pollutants
SELECT 
    season,
    ROUND(AVG(co_gt), 2) AS avg_co,
    ROUND(AVG(c6h6_gt), 2) AS avg_benzene,
    ROUND(AVG(nox_gt), 2) AS avg_nox,
    ROUND(AVG(temperature), 1) AS avg_temperature_c,
    ROUND(AVG(relative_humidity), 1) AS avg_humidity_pct
FROM air_quality_curated
GROUP BY season
ORDER BY avg_co DESC;

-- -----------------------------------------------------------------------------
-- OBJECTIVE 2: Sensor Calibration & Environmental Interference Analysis
-- -----------------------------------------------------------------------------

-- Widget 2.1: Sensor Calibration Scatter Data (PT08.S2 vs Benzene Analyzer)
SELECT 
    timestamp,
    sensor_s2_nmhc AS titania_sensor_response,
    c6h6_gt AS true_benzene_analyzer_ug_m3,
    temperature,
    relative_humidity
FROM air_quality_curated
WHERE sensor_s2_nmhc IS NOT NULL AND c6h6_gt IS NOT NULL
ORDER BY timestamp
LIMIT 2000;

-- Widget 2.2: Sensor Thermal Drift across Temperature Brackets
SELECT 
    CASE 
        WHEN temperature < 10.0 THEN '< 10 C (Cold Inversion)'
        WHEN temperature BETWEEN 10.0 AND 25.0 THEN '10 - 25 C (Moderate)'
        ELSE '> 25 C (Warm Summer)'
    END AS temperature_bracket,
    ROUND(AVG(sensor_s1_co), 1) AS mean_sensor_s1_co,
    ROUND(AVG(co_gt), 2) AS mean_co_ground_truth,
    ROUND(AVG(sensor_s2_nmhc), 1) AS mean_sensor_s2_nmhc,
    ROUND(AVG(c6h6_gt), 2) AS mean_benzene_ground_truth,
    COUNT(*) AS record_count
FROM air_quality_curated
WHERE temperature IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- -----------------------------------------------------------------------------
-- OBJECTIVE 3: Environmental Health Compliance & EU Limit Exceedances
-- -----------------------------------------------------------------------------

-- Widget 3.1: Executive Compliance KPI Scorecard
SELECT 
    COUNT(no2_gt) AS total_valid_no2_hours,
    SUM(CASE WHEN no2_gt > 200.0 THEN 1 ELSE 0 END) AS no2_limit_violations,
    ROUND(SUM(CASE WHEN no2_gt > 200.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(no2_gt), 2) AS no2_violation_rate_pct,
    SUM(CASE WHEN co_gt > 10.0 THEN 1 ELSE 0 END) AS severe_co_spikes,
    SUM(CASE WHEN c6h6_gt > 10.0 THEN 1 ELSE 0 END) AS high_risk_benzene_hours
FROM air_quality_curated;

-- Widget 3.2: Monthly Exceedance Timeline (EU NO2 > 200 ug/m3)
SELECT 
    year,
    month,
    CONCAT(year, '-', LPAD(month, 2, '0')) AS year_month,
    SUM(CASE WHEN no2_gt > 200.0 THEN 1 ELSE 0 END) AS no2_violations,
    ROUND(SUM(CASE WHEN no2_gt > 200.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(no2_gt), 2) AS violation_rate_pct,
    ROUND(AVG(no2_gt), 2) AS monthly_mean_no2,
    ROUND(AVG(co_gt), 2) AS monthly_mean_co
FROM air_quality_curated
GROUP BY year, month
ORDER BY year, month;

-- Widget 3.3: 8-Hour Rolling Average Carbon Monoxide (EU Standard Compliance)
WITH rolling_calc AS (
    SELECT 
        timestamp,
        co_gt,
        ROUND(AVG(co_gt) OVER (
            ORDER BY timestamp 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ), 2) AS co_8h_moving_avg
    FROM air_quality_curated
    WHERE co_gt IS NOT NULL
)
SELECT 
    timestamp,
    co_gt AS hourly_co,
    co_8h_moving_avg,
    CASE WHEN co_8h_moving_avg > 10.0 THEN 1 ELSE 0 END AS eu_8h_standard_violation
FROM rolling_calc
ORDER BY timestamp DESC
LIMIT 500;
