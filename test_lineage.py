"""
Test script to verify lineage functionality works correctly
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

def test_lineage():
    """Test lineage functionality"""
    try:
        from src.pipeline_engine import DataPipeline
        print("✅ Successfully imported DataPipeline")
        
        # Initialize pipeline
        pipeline = DataPipeline()
        print("✅ Successfully initialized pipeline")
        
        # Check if sample data exists
        if not os.path.exists("sample_data/bank_statement.csv"):
            print("⚠️ Sample data not found, creating it...")
            import pandas as pd
            os.makedirs("sample_data", exist_ok=True)
            
            # Create minimal sample data
            data = {
                'transaction_id': ['TXN001', 'TXN002'],
                'date': ['2024-01-01', '2024-01-02'],
                'amount': [1000.00, -500.00],
                'reference': ['REF001', 'REF002']
            }
            df = pd.DataFrame(data)
            df.to_csv("sample_data/bank_statement.csv", index=False)
            print("✅ Created sample data")
        
        # Test loading data
        print("📥 Loading data to generate lineage events...")
        df, event_id = pipeline.load_data("sample_data/bank_statement.csv", "csv")
        print(f"✅ Loaded data, generated event: {event_id}")
        
        # Test lineage summary
        print("📊 Testing lineage summary...")
        summary = pipeline.lineage_tracker.get_lineage_summary()
        print(f"✅ Summary: {summary}")
        
        # Test getting lineage logs
        events = pipeline.get_lineage_logs()
        print(f"✅ Retrieved {len(events)} lineage events")
        
        # Cleanup
        pipeline.stop()
        print("✅ Test completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_lineage()
