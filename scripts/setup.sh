#!/bin/bash
# Setup script for AI-ETL Framework

set -e  # Exit on error

echo "=================================================="
echo "AI-ETL Framework - Setup Script"
echo "=================================================="
echo ""

# Check Python version
echo "Checking Python version..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo "Found: $PYTHON_VERSION"
else
    echo "Error: python3 not found. Please install Python 3.11 or higher."
    exit 1
fi

# Create virtual environment
echo ""
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip --quiet

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install -r requirements.txt --quiet
echo "✓ Dependencies installed"

# Install dev dependencies (optional)
read -p "Install development dependencies? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    pip install -r requirements-dev.txt --quiet
    echo "✓ Development dependencies installed"
fi

# Create necessary directories
echo ""
echo "Creating directories..."
mkdir -p data
mkdir -p output
mkdir -p logs
mkdir -p .state
mkdir -p .errors
echo "✓ Directories created"

# Copy .env.example to .env if it doesn't exist
echo ""
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "✓ Created .env file from template"
    echo "  Please edit .env with your configuration"
else
    echo "✓ .env file already exists"
fi

echo ""
echo "=================================================="
echo "Setup Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "  1. Activate the virtual environment:"
echo "     source venv/bin/activate"
echo ""
echo "  2. Edit .env with your configuration (if needed)"
echo ""
echo "  3. Run the example pipeline:"
echo "     python examples/simple_csv_pipeline.py"
echo ""
echo "  4. View the documentation:"
echo "     - README.md - Getting started"
echo "     - docs/ARCHITECTURE.md - System design"
echo "     - docs/TECHNICAL_SPEC.md - Technical details"
echo ""
