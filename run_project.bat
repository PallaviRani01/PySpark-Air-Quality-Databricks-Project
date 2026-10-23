@echo off
REM ==============================================================================
REM  Air Quality Analytics Project - Windows CMD Runner
REM  PySpark ETL, KPI Development & Dashboard Generation
REM ==============================================================================

echo [CMD] Starting Air Quality PySpark Project Pipeline...
echo [CMD] Current Directory: %CD%

REM Unset mismatched SPARK_HOME if defined to avoid version conflicts on Windows
set "SPARK_HOME="

REM Verify Python
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python is not found in PATH! Please install Python or add it to PATH.
    pause
    exit /b 1
)

REM Execute the PySpark pipeline
echo [CMD] Executing PySpark ETL, KPI Engine, and Visualizer...
python run_pipeline.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ==============================================================================
    echo  [SUCCESS] All PySpark stages and KPI metrics completed successfully!
    echo  [NOTE] Visualizations and Dashboard files have been generated:
    echo    1. Images:      visualizations\*.png
    echo    2. VS Code:     notebooks\air_quality_vscode_dashboard.ipynb
    echo    3. Web Preview: visualizations\dashboard.html
    echo ==============================================================================
) else (
    echo.
    echo [ERROR] Pipeline failed with error code %ERRORLEVEL%.
)
