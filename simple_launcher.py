#!/usr/bin/env python3
"""
Simple launcher for the Bank Reconciliation Pipeline
This ensures proper path setup and environment configuration
"""
import os
import sys
import subprocess
from pathlib import Path

def setup_environment():
    """Setup Python path and environment"""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent.absolute()
    src_dir = script_dir / "src"
    
    # Add src directory to Python path
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    
    # Change to the project directory
    os.chdir(script_dir)
    
    print(f"📁 Project directory: {script_dir}")
    print(f"🐍 Python executable: {sys.executable}")
    print(f"📦 Python path includes: {src_dir}")

def test_imports():
    """Test that required modules can be imported"""
    try:
        import data_lineage
        import pipeline_engine
        import sample_data_generator
        print("✅ All modules imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure dependencies are installed: pip install -r requirements.txt")
        return False

def start_streamlit():
    """Start the Streamlit application"""
    app_path = Path(__file__).parent / "app" / "streamlit_app.py"
    
    print("🚀 Starting Streamlit application...")
    print("🔗 URL: http://localhost:8501")
    print("🛑 Press Ctrl+C to stop")
    
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run",
            str(app_path), "--server.port=8501"
        ])
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user")
    except FileNotFoundError:
        print("❌ Streamlit not found. Install it with: pip install streamlit")
    except Exception as e:
        print(f"❌ Error starting application: {e}")

def main():
    """Main function"""
    print("🏦 Bank Reconciliation Pipeline Launcher")
    print("=" * 50)
    
    # Setup environment
    setup_environment()
    
    # Test imports
    if not test_imports():
        print("\n💡 To fix import issues:")
        print("   1. Make sure you're in the project directory")
        print("   2. Install dependencies: pip install -r requirements.txt")
        print("   3. Try running: python simple_launcher.py")
        return 1
    
    # Start the application
    start_streamlit()
    return 0

if __name__ == "__main__":
    sys.exit(main())
