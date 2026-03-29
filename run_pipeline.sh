#!/bin/bash
# Retail ETL Pipeline Scheduler Script for Linux/Mac
# This script activates the virtual environment and runs the pipeline

echo "========================================"
echo "Retail ETL Pipeline Starting..."
echo "========================================"

# Change to script directory
cd "$(dirname "$0")"

# Activate virtual environment
source .venv/bin/activate

# Run the pipeline
python main.py

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "========================================"
    echo "Pipeline completed successfully!"
    echo "========================================"
else
    echo ""
    echo "========================================"
    echo "Pipeline failed with error code $?"
    echo "Check logs/pipeline.log for details"
    echo "========================================"
    exit 1
fi
