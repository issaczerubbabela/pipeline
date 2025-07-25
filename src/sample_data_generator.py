import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

class SampleDataGenerator:
    """Generate sample bank reconciliation data for testing"""
    
    def __init__(self, seed: int = 42):
        self.fake = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
        random.seed(seed)
    
    def generate_bank_statement(self, num_records: int = 1000) -> pd.DataFrame:
        """Generate sample bank statement data"""
        
        data = []
        start_date = datetime.now() - timedelta(days=30)
        
        for i in range(num_records):
            transaction_date = start_date + timedelta(days=random.randint(0, 30))
            transaction_amount = round(random.uniform(-5000, 10000), 2)
            
            record = {
                'transaction_id': f"TXN{i+1:06d}",
                'transaction_date': transaction_date.strftime('%Y-%m-%d'),
                'description': self.fake.sentence(nb_words=6),
                'amount': transaction_amount,
                'balance': round(random.uniform(1000, 50000), 2),
                'reference_number': self.fake.bothify(text='REF###??###'),
                'transaction_type': random.choice(['DEBIT', 'CREDIT']),
                'channel': random.choice(['ATM', 'ONLINE', 'BRANCH', 'MOBILE']),
                'currency': 'USD'
            }
            data.append(record)
        
        return pd.DataFrame(data)
    
    def generate_general_ledger(self, bank_statement_df: pd.DataFrame, 
                               match_rate: float = 0.85) -> pd.DataFrame:
        """Generate general ledger data with configurable match rate"""
        
        # Start with a subset of bank statement records
        num_matching = int(len(bank_statement_df) * match_rate)
        matching_records = bank_statement_df.sample(n=num_matching).copy()
        
        # Modify some data to create reconciliation challenges
        gl_data = []
        
        for _, record in matching_records.iterrows():
            gl_record = {
                'gl_transaction_id': f"GL{record['transaction_id'][3:]}",
                'posting_date': record['transaction_date'],
                'account_code': random.choice(['1001', '1002', '2001', '3001']),
                'account_name': random.choice(['Cash', 'Checking Account', 'Savings', 'Revenue']),
                'debit_amount': abs(record['amount']) if record['transaction_type'] == 'DEBIT' else 0,
                'credit_amount': abs(record['amount']) if record['transaction_type'] == 'CREDIT' else 0,
                'description': record['description'],
                'reference_number': record['reference_number'],
                'currency': record['currency'],
                'source_document': f"BANK_STMT_{record['transaction_date'][:7]}"
            }
            
            # Introduce some discrepancies for testing
            if random.random() < 0.1:  # 10% of records have amount discrepancies
                variance = random.uniform(0.1, 10.0)
                gl_record['debit_amount'] += variance if gl_record['debit_amount'] > 0 else 0
                gl_record['credit_amount'] += variance if gl_record['credit_amount'] > 0 else 0
            
            if random.random() < 0.05:  # 5% of records have date discrepancies
                date_obj = datetime.strptime(record['transaction_date'], '%Y-%m-%d')
                new_date = date_obj + timedelta(days=random.randint(1, 3))
                gl_record['posting_date'] = new_date.strftime('%Y-%m-%d')
            
            gl_data.append(gl_record)
        
        # Add some GL records that don't match bank statement
        num_additional = random.randint(50, 150)
        for i in range(num_additional):
            additional_record = {
                'gl_transaction_id': f"GL{len(bank_statement_df) + i + 1:06d}",
                'posting_date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
                'account_code': random.choice(['1001', '1002', '2001', '3001']),
                'account_name': random.choice(['Cash', 'Checking Account', 'Savings', 'Revenue']),
                'debit_amount': round(random.uniform(0, 5000), 2),
                'credit_amount': 0,
                'description': self.fake.sentence(nb_words=4),
                'reference_number': self.fake.bothify(text='GL###??###'),
                'currency': 'USD',
                'source_document': 'MANUAL_ENTRY'
            }
            gl_data.append(additional_record)
        
        return pd.DataFrame(gl_data)
    
    def generate_sample_datasets(self, output_dir: str = "sample_data"):
        """Generate and save sample datasets"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate bank statement
        print("Generating bank statement data...")
        bank_df = self.generate_bank_statement(1000)
        bank_path = os.path.join(output_dir, "bank_statement.csv")
        bank_df.to_csv(bank_path, index=False)
        print(f"Bank statement saved to: {bank_path}")
        
        # Generate general ledger
        print("Generating general ledger data...")
        gl_df = self.generate_general_ledger(bank_df, match_rate=0.85)
        gl_path = os.path.join(output_dir, "general_ledger.csv")
        gl_df.to_csv(gl_path, index=False)
        print(f"General ledger saved to: {gl_path}")
        
        # Generate Excel versions
        bank_excel_path = os.path.join(output_dir, "bank_statement.xlsx")
        gl_excel_path = os.path.join(output_dir, "general_ledger.xlsx")
        
        bank_df.to_excel(bank_excel_path, index=False)
        gl_df.to_excel(gl_excel_path, index=False)
        
        print(f"Excel files saved to: {bank_excel_path}, {gl_excel_path}")
        
        # Generate data quality issues for testing validation
        print("Generating data with quality issues...")
        
        # Bank statement with issues
        bank_with_issues = bank_df.copy()
        # Introduce null values
        bank_with_issues.loc[bank_with_issues.sample(frac=0.05).index, 'transaction_id'] = None
        bank_with_issues.loc[bank_with_issues.sample(frac=0.03).index, 'amount'] = None
        # Introduce duplicate IDs
        duplicate_indices = bank_with_issues.sample(frac=0.02).index
        bank_with_issues.loc[duplicate_indices, 'transaction_id'] = bank_with_issues.iloc[0]['transaction_id']
        
        bank_issues_path = os.path.join(output_dir, "bank_statement_with_issues.csv")
        bank_with_issues.to_csv(bank_issues_path, index=False)
        print(f"Bank statement with issues saved to: {bank_issues_path}")
        
        return {
            'bank_statement': bank_path,
            'general_ledger': gl_path,
            'bank_statement_excel': bank_excel_path,
            'general_ledger_excel': gl_excel_path,
            'bank_statement_with_issues': bank_issues_path
        }

if __name__ == "__main__":
    generator = SampleDataGenerator()
    paths = generator.generate_sample_datasets()
    print("\nSample datasets generated successfully!")
    print("File paths:", paths)
