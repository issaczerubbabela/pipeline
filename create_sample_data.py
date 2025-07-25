"""
Simple Demo Script to Generate Data Lineage Events
This version avoids complex imports and works directly with the existing structure.
"""

def create_sample_data():
    """Create simple sample data files"""
    import pandas as pd
    import os
    
    # Create sample_data directory if it doesn't exist
    os.makedirs("sample_data", exist_ok=True)
    
    # Create simple bank statement data
    bank_data = {
        'transaction_id': ['TXN001', 'TXN002', 'TXN003', 'TXN004', 'TXN005'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        'amount': [1000.00, -500.00, 750.00, -200.00, 1500.00],
        'description': ['Deposit', 'Withdrawal', 'Transfer', 'Fee', 'Deposit'],
        'reference': ['REF001', 'REF002', 'REF003', 'REF004', 'REF005']
    }
    
    bank_df = pd.DataFrame(bank_data)
    bank_df.to_csv("sample_data/bank_statement.csv", index=False)
    
    # Create simple general ledger data
    ledger_data = {
        'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        'account': ['Cash', 'Cash', 'Cash', 'Cash', 'Cash'],
        'amount': [1000.00, -500.00, 750.00, -200.00, 1500.00],
        'reference': ['REF001', 'REF002', 'REF003', 'REF004', 'REF005'],
        'description': ['Bank Deposit', 'Bank Withdrawal', 'Bank Transfer', 'Bank Fee', 'Bank Deposit']
    }
    
    ledger_df = pd.DataFrame(ledger_data)
    ledger_df.to_csv("sample_data/general_ledger.csv", index=False)
    
    print("✅ Sample data files created:")
    print("   - sample_data/bank_statement.csv")
    print("   - sample_data/general_ledger.csv")

if __name__ == "__main__":
    print("🎯 Creating Simple Sample Data...")
    create_sample_data()
    print("\n💡 Sample data is ready!")
    print("Now you can use the demo buttons in the web application:")
    print("1. Start web app: python main.py --mode web")
    print("2. Go to Data Lineage tab")
    print("3. Click the demo buttons")
