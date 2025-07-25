@echo off
echo =====================================
echo Bank Reconciliation Pipeline
echo =====================================

if not exist "venv\Scripts\activate.bat" (
    echo Virtual environment not found. Running setup...
    call setup.bat
    if %errorlevel% neq 0 exit /b 1
)

echo.
echo Activating virtual environment...
call venv\Scripts\activate.bat

echo.
echo Starting Bank Reconciliation Pipeline Web Application...
echo Open your browser to: http://localhost:8501
echo Press Ctrl+C to stop the application
echo.

python main.py --mode web
