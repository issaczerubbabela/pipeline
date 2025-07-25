@echo off
echo 🎯 Bank Reconciliation Pipeline - Lineage Demo
echo.
echo This script will generate sample lineage events that you can view in the web UI.
echo.

echo 📊 Generating sample data and lineage events...
python generate_lineage_demo.py --quick

echo.
echo 🎉 Demo completed! 
echo.
echo 💡 Next steps:
echo    1. Start the web application: python main.py --mode web
echo    2. Go to the "Data Lineage" tab to view the generated events
echo.
pause
