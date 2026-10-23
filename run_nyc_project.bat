@echo off
REM ==============================================================================
REM  NYC Air Quality Analytics Project Runner (Windows CMD)
REM  Target CSV: C:\Users\LENOVO\Downloads\Air_Quality.csv
REM  Video: https://www.youtube.com/watch?v=uz3LybMH7II
REM ==============================================================================

echo [CMD] Starting NYC Air Quality PySpark Pipeline...
echo [CMD] Target Dataset: C:\Users\LENOVO\Downloads\Air_Quality.csv

set "SPARK_HOME="

python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python not found in PATH!
    pause
    exit /b 1
)

python run_nyc_pipeline.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ==============================================================================
    echo  [SUCCESS] All PySpark stages and KPI metrics completed successfully!
    echo  [NOTE] Visualizations generated in:
    echo    1. visualizations\nyc_annual_pollutant_trend.png
    echo    2. visualizations\nyc_borough_comparison.png
    echo    3. visualizations\nyc_top10_polluted_community_districts.png
    echo    4. visualizations\nyc_asthma_morbidity_by_borough.png
    echo ==============================================================================
) else (
    echo [ERROR] Pipeline failed with error code %ERRORLEVEL%.
)
