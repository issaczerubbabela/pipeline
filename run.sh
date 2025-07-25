#!/bin/bash

echo "====================================="
echo "Bank Reconciliation Pipeline"
echo "====================================="

if [ ! -f "venv/bin/activate" ]; then
    echo "Virtual environment not found. Running setup..."
    bash setup.sh
    if [ $? -ne 0 ]; then exit 1; fi
fi

echo ""
echo "Activating virtual environment..."
source venv/bin/activate

echo ""
echo "Starting Bank Reconciliation Pipeline Web Application..."
echo "Open your browser to: http://localhost:8501"
echo "Press Ctrl+C to stop the application"
echo ""

python main.py --mode web
