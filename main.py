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
    
    subprocess.run([
        sys.executable, "-m", "streamlit", "run", 
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
        
        # Define validation rules
        validation_rules = [
            {'name': 'transaction_id_not_null', 'type': 'not_null', 'column': 'transaction_id'},
            {'name': 'amount_not_null', 'type': 'not_null', 'column': 'amount'},
            {'name': 'amount_range', 'type': 'range', 'column': 'amount', 'min_value': -10000, 'max_value': 50000}
        ]
        
        # Run validation
        print("✅ Running validation on bank statement...")
        bank_validation, _ = pipeline.run_validation(bank_df, validation_rules, bank_event_id)
        
        print("✅ Running validation on general ledger...")
        gl_validation, _ = pipeline.run_validation(gl_df, validation_rules[:2], gl_event_id)  # Skip amount range for GL
        
        # Run reconciliation
        print("🔄 Running reconciliation...")
        reconciliation_results = pipeline.run_reconciliation(
            bank_df, gl_df,
            join_keys=['reference_number'],
            compare_columns=['amount'],
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
        
        # Export reconciliation results
        if reconciliation_results['matched_df']:
            pipeline.export_results(
                reconciliation_results['matched_df'], 
                os.path.join(output_dir, "matched_records"),
                "csv"
            )
        
        # Export lineage
        pipeline.lineage_tracker.export_lineage(os.path.join(output_dir, "lineage"))
        
        print("✅ Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        raise
    
    finally:
        pipeline.stop()

def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(description="Bank Reconciliation Pipeline")
    parser.add_argument(
        "--mode", 
        choices=["web", "cli"],
        default="web",
        help="Run mode: web (Streamlit UI) or cli (command line)"
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
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user")
    except Exception as e:
        print(f"❌ Application failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
