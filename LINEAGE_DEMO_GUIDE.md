# Data Lineage Demo Guide

The "No lineage events recorded yet" message appears because lineage events are only created when you actually run pipeline operations. Here are several ways to generate lineage events that you can view in the UI:

## 🔧 Prerequisites

**IMPORTANT**: Install dependencies first!
```powershell
# Install all required packages
pip install -r requirements.txt

# OR use the VS Code task
# Press Ctrl+Shift+P -> "Tasks: Run Task" -> "Install Pipeline Dependencies"
```

## 🚀 Quick Options (Choose one)

### Option 1: Use Simple Launcher (RECOMMENDED) 
```powershell
# This handles all path setup automatically
python simple_launcher.py
```

### Option 2: Use Batch File (Windows)
```powershell
# This activates virtual environment and starts the app
start_app.bat
```

### Option 3: Use Demo Buttons in Web UI
1. Create sample data: `python create_sample_data.py`
2. Start the web application: `python simple_launcher.py`
3. Go to the "Data Lineage" tab
4. Click one of the demo buttons:
   - **🚀 Quick Demo**: Loads sample data (generates 2 lineage events)
   - **📊 Validation Demo**: Runs validation (generates 4+ lineage events)
   - **🔄 Reset Pipeline**: Clears all events to start fresh

### Option 2: Create Sample Data Only
```powershell
# Create sample data files
python create_sample_data.py
```
Then use the web UI to upload and process the files manually.

### Option 3: Use Main Application Demo Mode
```powershell
python main.py --mode demo
```

### Option 4: Run Batch File (Windows)
```powershell
demo_lineage.bat
```

## 📋 What Each Demo Generates

### Quick Demo (2 events)
- **SOURCE event**: Loading bank_statement.csv
- **SOURCE event**: Loading general_ledger.csv

### Validation Demo (4+ events)
- 2 SOURCE events (loading data)
- **VALIDATION event**: Bank statement validation
- **VALIDATION event**: General ledger validation

### Full Pipeline Demo (6+ events)
- 2 SOURCE events (loading data)
- 2 VALIDATION events (data validation)
- **RECONCILIATION event**: Comparing datasets
- **TRANSFORMATION events**: Any data transformations

## 🎯 Manual Operations (Advanced)

You can also generate lineage events manually by using the web interface:

1. **Upload Data**: Use the "Data Upload" tab to upload CSV/Excel files
2. **Configure Validation**: Set up validation rules in "Validation Rules" tab
3. **Run Reconciliation**: Configure and run reconciliation in "Reconciliation" tab

Each of these operations will automatically generate corresponding lineage events.

## 🔍 Viewing Lineage Events

After generating events, you can view them in the web UI:

1. Go to **Data Lineage** tab
2. See event summary with counts and types
3. View detailed event timeline
4. Export lineage data if needed

## 📊 Types of Lineage Events

- **SOURCE**: Data loading from files
- **VALIDATION**: Data quality checks and rule applications
- **TRANSFORMATION**: Data processing and modifications
- **RECONCILIATION**: Comparing and matching datasets

## 🛠️ Troubleshooting

**Problem**: "Import error: attempted relative import with no known parent package"
- **Solution**: Use `python simple_launcher.py` instead of `python main.py --mode web`

**Problem**: TypeError: max() got an unexpected keyword argument 'key'
- **Solution**: Fixed! The lineage summary method now handles edge cases properly

**Problem**: "No module named 'pyspark'" error  
- **Solution**: Install dependencies first: `pip install -r requirements.txt` or use the task "Install Pipeline Dependencies"

**Problem**: Import errors when starting the app
- **Solution**: Use `simple_launcher.py` or `start_app.bat` which handle path setup automatically

**Problem**: "Port 8501 is already in use"
- **Solution**: Stop existing Streamlit processes or use a different port

**Problem**: Lineage events disappear after clicking buttons
- **Solution**: Fixed! Events now persist in session state

**Problem**: No sample data found
- **Solution**: Run `python create_sample_data.py` to create sample files

## 💡 Pro Tips

1. **Start with Quick Demo**: Run the quick demo first to see basic lineage events
2. **Use Web UI Buttons**: The demo buttons in the web UI are the easiest way to generate events
3. **Check Logs**: Monitor the console output to see what events are being generated
4. **Persistent Events**: Lineage events stay in memory while the application is running

## 🔄 Next Steps

After generating lineage events:
1. Explore the lineage visualization in the web UI
2. Try uploading your own data files
3. Configure custom validation rules
4. Export lineage data for external analysis
