@echo off
echo 🚀 Starting Bank Reconciliation Pipeline Web Application...
echo.

REM Activate virtual environment
call "%~dp0venv\Scripts\activate.bat"

REM Check if activation was successful
if not defined VIRTUAL_ENV (
    echo ❌ Failed to activate virtual environment
    echo Please run: python -m venv venv
    echo Then: venv\Scripts\activate.bat
    echo And: pip install -r requirements.txt
    pause
    exit /b 1
)

echo ✅ Virtual environment activated: %VIRTUAL_ENV%
echo 📦 Installing/updating dependencies...
pip install -r requirements.txt --quiet

echo 📊 Starting Streamlit application...
echo 🔗 Opening in browser: http://localhost:8501
echo 🛑 Press Ctrl+C to stop the application
echo.

python -m streamlit run app\streamlit_app.py --server.port=8501

pause
