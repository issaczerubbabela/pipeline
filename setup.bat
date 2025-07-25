@echo off
echo =====================================
echo Bank Reconciliation Pipeline Setup
echo =====================================

echo.
echo Checking Python installation...
python --version
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://python.org
    pause
    exit /b 1
)

echo.
echo Checking Java installation...
java -version
if %errorlevel% neq 0 (
    echo ERROR: Java is not installed or not in PATH
    echo Please install Java 8+ from https://openjdk.java.net/
    pause
    exit /b 1
)

echo.
echo Creating virtual environment...
python -m venv venv
if %errorlevel% neq 0 (
    echo ERROR: Failed to create virtual environment
    pause
    exit /b 1
)

echo.
echo Activating virtual environment...
call venv\Scripts\activate.bat

echo.
echo Upgrading pip...
python -m pip install --upgrade pip

echo.
echo Installing dependencies...
echo Trying full requirements first...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo Full installation failed. Trying minimal requirements...
    pip install -r requirements-minimal.txt
    if %errorlevel% neq 0 (
        echo ERROR: Failed to install dependencies
        echo.
        echo Please try installing manually:
        echo   pip install pyspark streamlit pandas plotly openpyxl faker
        pause
        exit /b 1
    )
    echo.
    echo ⚠️  Minimal installation completed. Some advanced features may not be available.
)

echo.
echo Creating output directories...
mkdir temp_data 2>nul
mkdir output 2>nul
mkdir lineage 2>nul
mkdir sample_data 2>nul

echo.
echo Generating sample data...
python -c "from src.sample_data_generator import SampleDataGenerator; SampleDataGenerator().generate_sample_datasets()"

echo.
echo =====================================
echo Setup completed successfully!
echo =====================================
echo.
echo To run the application:
echo   1. Activate virtual environment: venv\Scripts\activate.bat
echo   2. Run web app: python main.py --mode web
echo   3. Or run CLI: python main.py --mode cli
echo.
pause
