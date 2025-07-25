"""
Test imports to diagnose the exact issue
"""
import sys
import os

print("Python path:")
for path in sys.path:
    print(f"  {path}")

print(f"\nCurrent working directory: {os.getcwd()}")
print(f"Script location: {os.path.dirname(os.path.abspath(__file__))}")

# Test 1: Try importing from src directly
print("\n=== Test 1: Direct import from src ===")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

try:
    from data_lineage import DataLineageTracker
    print("✅ Successfully imported DataLineageTracker")
except Exception as e:
    print(f"❌ Failed to import DataLineageTracker: {e}")

try:
    from pipeline_engine import DataPipeline
    print("✅ Successfully imported DataPipeline")
except Exception as e:
    print(f"❌ Failed to import DataPipeline: {e}")

# Test 2: Try importing as app would
print("\n=== Test 2: App-style import ===")
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    import pipeline_engine
    print("✅ Successfully imported pipeline_engine module")
except Exception as e:
    print(f"❌ Failed to import pipeline_engine module: {e}")

print("\n=== Files in src directory ===")
src_dir = os.path.join(os.path.dirname(__file__), 'src')
if os.path.exists(src_dir):
    for file in os.listdir(src_dir):
        print(f"  {file}")
else:
    print("  src directory not found!")
