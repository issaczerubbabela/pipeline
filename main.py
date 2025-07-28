#!/usr/bin/env python3
"""
Bank Reconciliation Pipeline - Main Application Runner
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('pipeline.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def run_streamlit_app():
    """Run the Streamlit web application"""
    import subprocess
    app_path = Path(__file__).parent / "app" / "streamlit_app.py"
    
    print("🚀 Starting Bank Reconciliation Pipeline Web Application...")
    print("📊 The application will open in your default web browser")
    print("🔗 URL: http://localhost:8501")
    print("🛑 Press Ctrl+C to stop the application")
    
    # Use the same Python executable that's running this script
    python_executable = sys.executable
    print(f"Using Python: {python_executable}")
    
    subprocess.run([
        python_executable, "-m", "streamlit", "run", 
        str(app_path), "--server.port=8501"
    ])

def run_cli_mode():
    """Run in command-line interface mode"""
    print("🔧 CLI Mode - Bank Reconciliation Pipeline")
    
    # Import pipeline components
    from pipeline_engine import DataPipeline
    from sample_data_generator import SampleDataGenerator
    
    # Generate sample data if needed
    generator = SampleDataGenerator()
    print("📁 Generating sample datasets...")
    sample_paths = generator.generate_sample_datasets()
    
    # Initialize pipeline
    print("⚙️ Initializing pipeline...")
    pipeline = DataPipeline()
    
    try:
        # Load sample data
        print("📊 Loading bank statement...")
        bank_df, bank_event_id = pipeline.load_data(sample_paths['bank_statement'])
        
        print("📊 Loading general ledger...")
        gl_df, gl_event_id = pipeline.load_data(sample_paths['general_ledger'])
        
        # Define validation rules for bank statement
        bank_validation_rules = [
            {'name': 'transaction_id_not_null', 'type': 'not_null', 'column': 'transaction_id'},
            {'name': 'amount_not_null', 'type': 'not_null', 'column': 'amount'},
            {'name': 'amount_range', 'type': 'range', 'column': 'amount', 'min_value': -10000, 'max_value': 50000}
        ]
        
        # Define validation rules for general ledger
        gl_validation_rules = [
            {'name': 'gl_transaction_id_not_null', 'type': 'not_null', 'column': 'gl_transaction_id'},
            {'name': 'debit_amount_not_null', 'type': 'not_null', 'column': 'debit_amount'},
            {'name': 'credit_amount_not_null', 'type': 'not_null', 'column': 'credit_amount'}
        ]
        
        # Run validation
        print("✅ Running validation on bank statement...")
        bank_validation, _ = pipeline.run_validation(bank_df, bank_validation_rules, bank_event_id)
        
        print("✅ Running validation on general ledger...")
        gl_validation, _ = pipeline.run_validation(gl_df, gl_validation_rules, gl_event_id)
        
        # Prepare GL data for reconciliation - create net amount column
        print("🔄 Preparing data for reconciliation...")
        
        # For GL: net_amount = debit_amount - credit_amount
        # (positive = debit, negative = credit, matching typical bank statement conventions)
        from pyspark.sql.functions import col, when
        gl_df_prepared = gl_df.withColumn(
            "net_amount", 
            col("debit_amount") - col("credit_amount")
        )
        
        # Show examples of different column comparison options
        print("\n💡 Available column comparison formats:")
        examples = pipeline.get_column_comparison_examples()
        print(f"   • Exact match (same name): '{examples['exact_match_same_name']}'")
        print(f"   • Different column names: '{examples['exact_match_different_names']}'")
        print(f"   • Numeric with tolerance: '{examples['numeric_with_tolerance']}'")
        print(f"   • Absolute value (ignore sign): '{examples['absolute_value_comparison']}'")
        print(f"   • Opposite signs (+ vs -): '{examples['opposite_sign_comparison']}'")
        print(f"   • Text comparison: '{examples['string_comparison']}'")
        
        # Option 1: Basic comparison (your original approach)
        basic_comparisons = ['amount:net_amount']
        
        # Option 2: Enhanced comparisons with actual available columns
        enhanced_comparisons = [
            'amount:net_amount:abs:0.01',           # Amount comparison (ignore sign, 1 cent tolerance)
            'currency:currency',                    # Currency comparison (same name in both)
            'description:description',              # Description comparison (same name in both) 
            'transaction_date:posting_date',        # Date fields (different names)
            'transaction_type:account_name',        # Transaction type vs account name
            'channel:source_document'               # Channel vs source document
        ]
        
        # Option 3: Test different column combinations
        test_comparisons = [
            'amount:net_amount:abs:0.01',           # Core amount comparison
            'transaction_id:gl_transaction_id',     # ID comparison (different names)
            'balance:debit_amount:abs',             # Balance vs debit amount
            'currency:currency',                    # Same column name
            'channel:account_code'                  # Channel vs account code
        ]
        
        # Option 4: Opposite sign comparison (when systems use different conventions)
        opposite_sign_comparisons = [
            'amount:net_amount:opposite:0.01',      # Expect opposite signs (bank: +100, GL: -100)
            'currency:currency',                    # Currency should match exactly
            'description:description'               # Description comparison
        ]
        
        # Option 5: Using helper method for complex scenarios
        advanced_comparisons = [
            pipeline.create_column_comparison('amount', 'net_amount', 'abs', 0.01),        # Absolute value
            pipeline.create_column_comparison('currency', 'currency', 'exact'),            # Exact match
            pipeline.create_column_comparison('transaction_date', 'posting_date', 'exact') # Date comparison
        ]
        
        # Choose which comparison set to use based on your data characteristics
        print(f"\n🔧 Available comparison strategies:")
        print(f"   1. Enhanced (real columns): {enhanced_comparisons}")
        print(f"   2. Test combinations: {test_comparisons}")
        print(f"   3. Opposite signs: {opposite_sign_comparisons}")
        print(f"   4. Advanced (helper methods): {len(advanced_comparisons)} comparisons")
        
        # For demonstration, let's use the enhanced comparisons with real column names
        print(f"\n🔧 Using enhanced column comparisons with real column names:")
        selected_comparisons = enhanced_comparisons
        for comp in selected_comparisons:
            print(f"   • {comp}")

        # Run reconciliation with enhanced column comparisons
        print("🔄 Running reconciliation...")
        reconciliation_results = pipeline.run_reconciliation(
            bank_df, gl_df_prepared,
            join_keys=['reference_number'],
            compare_columns=selected_comparisons,
            source1_event_id=bank_event_id,
            source2_event_id=gl_event_id
        )
        
        # Print results
        print("\n" + "="*50)
        print("📊 PIPELINE RESULTS")
        print("="*50)
        
        print(f"\n✅ Bank Statement Validation:")
        print(f"   Total Rows: {bank_validation['total_rows']}")
        print(f"   Passed Rules: {bank_validation['passed_rules']}")
        print(f"   Failed Rules: {bank_validation['failed_rules']}")
        
        print(f"\n✅ General Ledger Validation:")
        print(f"   Total Rows: {gl_validation['total_rows']}")
        print(f"   Passed Rules: {gl_validation['passed_rules']}")
        print(f"   Failed Rules: {gl_validation['failed_rules']}")
        
        print(f"\n🔄 Reconciliation Results:")
        results = reconciliation_results['results']
        print(f"   Match Rate: {results['match_rate']:.2%}")
        print(f"   Matched Records: {results['matched_records']}")
        print(f"   Missing in Bank: {results['missing_in_source1']}")
        print(f"   Missing in GL: {results['missing_in_source2']}")
        print(f"   Discrepancies: {results['discrepancies']}")
        
        # Show lineage summary
        print(f"\n🔗 Data Lineage Summary:")
        lineage_summary = pipeline.lineage_tracker.get_lineage_summary()
        print(f"   Total Events: {lineage_summary['total_events']}")
        for event_type, count in lineage_summary['event_types'].items():
            print(f"   {event_type}: {count}")
        
        # Export results
        print(f"\n📁 Exporting results...")
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Export reconciliation results - skip export for now due to Hadoop/Windows issues
        if reconciliation_results['matched_df']:
            print("📊 Matched records DataFrame is ready for export")
            print("   (Export temporarily disabled due to Hadoop/Windows compatibility)")
            # matched_df = reconciliation_results['matched_df']
            # pipeline.export_results(
            #     matched_df, 
            #     os.path.join(output_dir, "matched_records"),
            #     "csv"
            # )
        
        # Export lineage - skip for now due to compatibility issues
        print("🔗 Lineage data is ready for export")
        print("   (Lineage export temporarily disabled due to compatibility)")
        # pipeline.lineage_tracker.export_lineage(os.path.join(output_dir, "lineage"))
        
        print("✅ Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        raise
    
    finally:
        pipeline.stop()

def run_demo_mode():
    """Run demo mode to generate lineage events"""
    print("🎯 Demo Mode - Generating Lineage Events")
    
    try:
        from generate_lineage_demo import quick_lineage_demo, demo_lineage_generation
        
        print("\nChoose demo type:")
        print("1. Quick Demo - Load sample data (generates ~2 lineage events)")
        print("2. Full Demo - Complete pipeline (generates ~6+ lineage events)")
        
        choice = input("Enter choice (1 or 2): ").strip()
        
        if choice == "1":
            pipeline = quick_lineage_demo()
        elif choice == "2":
            pipeline = demo_lineage_generation()
        else:
            print("Invalid choice, running quick demo...")
            pipeline = quick_lineage_demo()
        
        print(f"\n🎉 Demo completed!")
        print("💡 You can now start the web application to view the lineage events:")
        print("   python main.py --mode web")
        
    except Exception as e:
        print(f"❌ Demo failed: {str(e)}")
        raise

def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(description="Bank Reconciliation Pipeline")
    parser.add_argument(
        "--mode", 
        choices=["web", "cli", "demo"],
        default="web",
        help="Run mode: web (Streamlit UI), cli (command line), or demo (generate lineage events)"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    try:
        if args.mode == "web":
            run_streamlit_app()
        elif args.mode == "cli":
            run_cli_mode()
        elif args.mode == "demo":
            run_demo_mode()
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user")
    except Exception as e:
        print(f"❌ Application failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
