#!/bin/bash

# Build script for Lambda functions
# Uses Python virtual environment to isolate dependencies
# Separates shared layer (pyarrow) from function code (handler only)

set -e

echo "Building Lambda functions with optimized packaging..."
echo ""

# Define paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$PROJECT_ROOT/venv"
LAMBDA_DIR="$PROJECT_ROOT/lambda"
LAYER_DIR="$LAMBDA_DIR/layers"

# Check if venv exists, create if not
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "✓ Virtual environment created"
else
    echo "✓ Using existing virtual environment"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Upgrade pip in venv
echo "Upgrading pip..."
pip install --upgrade pip -q

# ============================================
# Build Shared Lambda Layer (pyarrow only)
# ============================================
echo ""
echo "=========================================="
echo "Building Shared Lambda Layer"
echo "=========================================="
echo "Installing pyarrow to layer..."
echo "(boto3 excluded - provided by AWS Lambda runtime)"
echo "(Using Linux-compatible wheels for AWS Lambda)"

# Clean previous layer
rm -rf "$LAYER_DIR/python" 2>/dev/null || true
rm -f "$LAYER_DIR/python_deps.zip" 2>/dev/null || true
mkdir -p "$LAYER_DIR/python"

# Install ONLY pyarrow (boto3 is provided by AWS)
# Use --platform and --only-binary to get Linux-compatible wheels for AWS Lambda
pip install pyarrow>=14.0.0 \
    --platform manylinux2014_x86_64 \
    --target "$LAYER_DIR/python" \
    --implementation cp \
    --python-version 3.11 \
    --only-binary=:all: \
    --upgrade \
    -q

# Create layer zip
cd "$LAYER_DIR"
zip -r python_deps.zip python -q
LAYER_SIZE=$(du -h python_deps.zip | cut -f1)
echo "✓ Lambda layer created: $LAYER_SIZE"

# Clean up unzipped files
rm -rf python/

cd "$PROJECT_ROOT"

# ============================================
# Build Individual Lambda Functions
# (Handler code ONLY - no dependencies)
# ============================================
echo ""
echo "=========================================="
echo "Building Lambda Functions"
echo "=========================================="
echo "(Dependencies from shared layer - packaging handler code only)"
echo ""

LAMBDA_FUNCTIONS=("bronze" "silver" "gold")

for func in "${LAMBDA_FUNCTIONS[@]}"; do
    FUNC_DIR="$LAMBDA_DIR/$func"
    DEPLOYMENT_ZIP="$FUNC_DIR/deployment.zip"
    
    echo "Building $func Lambda..."
    
    # Clean previous deployment package
    rm -f "$DEPLOYMENT_ZIP" 2>/dev/null || true
    
    # Zip ONLY the handler.py file
    # Dependencies come from the shared layer
    cd "$FUNC_DIR"
    zip -r "$DEPLOYMENT_ZIP" handler.py -q
    
    FUNC_SIZE=$(du -h "$DEPLOYMENT_ZIP" | cut -f1)
    echo "✓ $func: $FUNC_SIZE (handler only)"
    
    cd "$PROJECT_ROOT"
done

# Deactivate venv
deactivate

echo ""
echo "=========================================="
echo "✅ Lambda build complete!"
echo "=========================================="
echo ""
echo "Deployment packages created:"
echo "  📦 Shared Layer: lambda/layers/python_deps.zip"
echo "  📄 Bronze:       lambda/bronze/deployment.zip"
echo "  📄 Silver:       lambda/silver/deployment.zip"
echo "  📄 Gold:         lambda/gold/deployment.zip"
echo ""
echo "Package sizes:"
du -h "$LAYER_DIR/python_deps.zip" | awk '{print "  Layer:  " $1}'
du -h "$LAMBDA_DIR/bronze/deployment.zip" | awk '{print "  Bronze: " $1}'
du -h "$LAMBDA_DIR/silver/deployment.zip" | awk '{print "  Silver: " $1}'
du -h "$LAMBDA_DIR/gold/deployment.zip" | awk '{print "  Gold:   " $1}'
echo ""
echo "💡 Each function uses the shared layer for dependencies"
echo "💡 boto3 is provided by AWS Lambda runtime (not packaged)"
echo ""
echo "Next steps:"
echo "  cd terraform"
echo "  terraform init && terraform apply"
echo ""
