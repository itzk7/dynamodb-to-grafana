#!/bin/bash

# Clean up build artifacts and virtual environment
# Run this if you want to start fresh

echo "Cleaning up build artifacts..."

# Remove deployment packages
rm -rf lambda/bronze/deployment.zip
rm -rf lambda/silver/deployment.zip
rm -rf lambda/gold/deployment.zip
rm -rf lambda/layers/python_deps.zip
rm -rf lambda/layers/python

# Remove installed dependencies from Lambda directories
cd lambda/bronze && find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
cd ../silver && find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
cd ../gold && find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
cd ../..

# Remove virtual environment
if [ -d "venv" ]; then
    echo "Removing virtual environment..."
    rm -rf venv
fi

echo ""
echo "✓ Clean complete!"
echo ""
echo "Run './scripts/build_lambda.sh' to rebuild"

