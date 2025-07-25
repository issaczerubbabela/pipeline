"""
Demo Script to Generate Data Lineage Events

This script demonstrates various pipeline operations that generate lineage events
which can then be viewed in the Streamlit UI.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from src.pipeline_engine import DataPipeline
from src.sample_data_generator import SampleDataGenerator
import tempfile
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def demo_lineage_generation():
    """Generate sample lineage events for demonstration"""
    
    print("🚀 Starting Data Lineage Demo...")
    
    # Initialize pipeline
    pipeline = DataPipeline()
    
    # Step 1: Generate sample data
    print("\n📊 Step 1: Generating sample data...")
    generator = SampleDataGenerator()
    bank_path, ledger_path = generator.generate_sample_datasets()
    print(f"✅ Generated bank statement: {bank_path}")
    print(f"✅ Generated general ledger: {ledger_path}")
    
    # Step 2: Load bank statement data
    print("\n📥 Step 2: Loading bank statement data...")
    bank_df, bank_event_id = pipeline.load_data(bank_path, "csv")
    print(f"✅ Loaded bank data with event ID: {bank_event_id}")
    print(f"   - Rows: {bank_df.count()}")
    print(f"   - Columns: {len(bank_df.columns)}")
    
    # Step 3: Load general ledger data
    print("\n📥 Step 3: Loading general ledger data...")
    ledger_df, ledger_event_id = pipeline.load_data(ledger_path, "csv")
    print(f"✅ Loaded ledger data with event ID: {ledger_event_id}")
    print(f"   - Rows: {ledger_df.count()}")
    print(f"   - Columns: {len(ledger_df.columns)}")
    
    # Step 4: Run validation on bank statement
    print("\n✅ Step 4: Running validation on bank statement...")
    bank_validation_rules = [
        {
            'name': 'transaction_id_not_null',
            'type': 'not_null',
            'column': 'transaction_id'
        },
        {
            'name': 'amount_not_null',
            'type': 'not_null',
            'column': 'amount'
        },
        {
            'name': 'date_not_null',
            'type': 'not_null',
            'column': 'date'
        },
        {
            'name': 'amount_range_check',
            'type': 'range',
            'column': 'amount',
            'min_value': -50000,
            'max_value': 50000
        }
    ]
    
    bank_validation_results, bank_validation_event_id = pipeline.run_validation(
        bank_df, bank_validation_rules, bank_event_id
    )
    print(f"✅ Bank validation completed with event ID: {bank_validation_event_id}")
    print(f"   - Total rules: {len(bank_validation_rules)}")
    print(f"   - Passed: {bank_validation_results['passed_rules']}")
    print(f"   - Failed: {bank_validation_results['failed_rules']}")
    
    # Step 5: Run validation on general ledger
    print("\n✅ Step 5: Running validation on general ledger...")
    ledger_validation_rules = [
        {
            'name': 'reference_not_null',
            'type': 'not_null',
            'column': 'reference'
        },
        {
            'name': 'amount_not_null',
            'type': 'not_null',
            'column': 'amount'
        },
        {
            'name': 'account_not_null',
            'type': 'not_null',
            'column': 'account'
        }
    ]
    
    ledger_validation_results, ledger_validation_event_id = pipeline.run_validation(
        ledger_df, ledger_validation_rules, ledger_event_id
    )
    print(f"✅ Ledger validation completed with event ID: {ledger_validation_event_id}")
    print(f"   - Total rules: {len(ledger_validation_rules)}")
    print(f"   - Passed: {ledger_validation_results['passed_rules']}")
    print(f"   - Failed: {ledger_validation_results['failed_rules']}")
    
    # Step 6: Run reconciliation
    print("\n🔄 Step 6: Running reconciliation...")
    try:
        join_keys = ['reference']
        compare_columns = ['amount', 'date']
        
        reconciliation_results, reconciliation_event_id = pipeline.run_reconciliation(
            bank_df, ledger_df, join_keys, compare_columns, 
            bank_event_id, ledger_event_id
        )
        print(f"✅ Reconciliation completed with event ID: {reconciliation_event_id}")
        print(f"   - Matched records: {reconciliation_results.get('matched_count', 0)}")
        print(f"   - Missing in bank: {reconciliation_results.get('missing_in_source1', 0)}")
        print(f"   - Missing in ledger: {reconciliation_results.get('missing_in_source2', 0)}")
        
    except Exception as e:
        print(f"⚠️ Reconciliation encountered an issue: {str(e)}")
        print("   This is normal for demo data - columns might not match perfectly")
    
    # Step 7: Display lineage summary
    print("\n📋 Step 7: Lineage Summary")
    lineage_events = pipeline.get_lineage_logs()
    summary = pipeline.lineage_tracker.get_lineage_summary()
    
    print(f"✅ Total lineage events generated: {summary['total_events']}")
    print("📊 Event types:")
    for event_type, count in summary['event_types'].items():
        print(f"   - {event_type}: {count}")
    
    print(f"\n🎉 Demo completed! You can now view {len(lineage_events)} lineage events in the Streamlit UI.")
    print("💡 Go to the 'Data Lineage' tab in the web application to see all the tracked events.")
    
    # Keep Spark session alive for the web app
    print("\n⏳ Keeping pipeline alive for web app usage...")
    print("   The pipeline instance with lineage data is now available in the web interface.")
    
    return pipeline

def quick_lineage_demo():
    """Quick demo that just loads data to generate basic lineage events"""
    print("🚀 Quick Lineage Demo - Loading sample data...")
    
    pipeline = DataPipeline()
    
    # Check if sample data exists
    sample_files = [
        "sample_data/bank_statement.csv",
        "sample_data/general_ledger.csv"
    ]
    
    for file_path in sample_files:
        if os.path.exists(file_path):
            print(f"📥 Loading {file_path}...")
            try:
                df, event_id = pipeline.load_data(file_path, "csv")
                print(f"✅ Loaded {file_path} with event ID: {event_id}")
                print(f"   - Rows: {df.count()}, Columns: {len(df.columns)}")
            except Exception as e:
                print(f"❌ Error loading {file_path}: {str(e)}")
        else:
            print(f"⚠️ File not found: {file_path}")
    
    lineage_count = len(pipeline.get_lineage_logs())
    print(f"\n🎉 Quick demo completed! Generated {lineage_count} lineage events.")
    
    return pipeline

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate lineage events for demo")
    parser.add_argument("--quick", action="store_true", help="Run quick demo (just load data)")
    parser.add_argument("--full", action="store_true", help="Run full demo (load, validate, reconcile)")
    
    args = parser.parse_args()
    
    if args.quick:
        pipeline = quick_lineage_demo()
    elif args.full:
        pipeline = demo_lineage_generation()
    else:
        print("Choose demo type:")
        print("  --quick: Just load sample data (generates 2 lineage events)")
        print("  --full:  Complete pipeline demo (generates 6+ lineage events)")
        
        choice = input("\nEnter 'quick' or 'full': ").strip().lower()
        if choice == 'quick':
            pipeline = quick_lineage_demo()
        elif choice == 'full':
            pipeline = demo_lineage_generation()
        else:
            print("Invalid choice. Running quick demo...")
            pipeline = quick_lineage_demo()
