#!/bin/bash

echo "====================================="
echo "Bank Reconciliation Pipeline Setup"
echo "====================================="

echo ""
echo "Checking Python installation..."
python3 --version
if [ $? -ne 0 ]; then
    echo "ERROR: Python 3 is not installed or not in PATH"
    echo "Please install Python 3.8+ from https://python.org"
    exit 1
fi

echo ""
echo "Checking Java installation..."
java -version
if [ $? -ne 0 ]; then
    echo "ERROR: Java is not installed or not in PATH"
    echo "Please install Java 8+ from https://openjdk.java.net/"
    exit 1
fi

echo ""
echo "Creating virtual environment..."
python3 -m venv venv
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to create virtual environment"
    exit 1
fi

echo ""
echo "Activating virtual environment..."
source venv/bin/activate

echo ""
echo "Upgrading pip..."
python -m pip install --upgrade pip

echo ""
echo "Installing dependencies..."
pip install -r requirements.txt
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to install dependencies"
    exit 1
fi

echo ""
echo "Creating output directories..."
mkdir -p temp_data output lineage sample_data

echo ""
echo "Generating sample data..."
python -c "from src.sample_data_generator import SampleDataGenerator; SampleDataGenerator().generate_sample_datasets()"

echo ""
echo "====================================="
echo "Setup completed successfully!"
echo "====================================="
echo ""
echo "To run the application:"
echo "  1. Activate virtual environment: source venv/bin/activate"
echo "  2. Run web app: python main.py --mode web"
echo "  3. Or run CLI: python main.py --mode cli"
