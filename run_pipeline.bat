@echo off
REM Retail ETL Pipeline Scheduler Script for Windows
REM This script activates the virtual environment and runs the pipeline

echo ========================================
echo Retail ETL Pipeline Starting...
echo ========================================

REM Change to script directory
cd /d "%~dp0"

REM Activate virtual environment
call .venv\Scripts\activate.bat

REM Run the pipeline
python main.py

REM Check exit code
if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo Pipeline completed successfully!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo Pipeline failed with error code %ERRORLEVEL%
    echo Check logs/pipeline.log for details
    echo ========================================
)

REM Keep window open if run manually
pause
