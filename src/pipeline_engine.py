# Fix for distutils deprecation in Python 3.12+
try:
    from . import distutils_fix
except ImportError:
    try:
        import distutils_fix
    except ImportError:
        pass

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from functools import reduce

# Handle both relative and absolute imports
try:
    from .data_lineage import DataLineageTracker
except ImportError:
    from data_lineage import DataLineageTracker

class ValidationEngine:
    """Handles data validation with configurable rules"""
    
    def __init__(self, spark_session: SparkSession, lineage_tracker: DataLineageTracker):
        self.spark = spark_session
        self.lineage_tracker = lineage_tracker
        self.logger = logging.getLogger(__name__)
    
    def validate_data(self, df, validation_rules: List[Dict], source_event_id: str):
        """Apply validation rules to DataFrame"""
        validation_results = {
            'total_rows': df.count(),
            'passed_rules': 0,
            'failed_rules': 0,
            'rule_results': []
        }
        
        for rule in validation_rules:
            rule_result = self._apply_validation_rule(df, rule)
            validation_results['rule_results'].append(rule_result)
            
            if rule_result['passed']:
                validation_results['passed_rules'] += 1
            else:
                validation_results['failed_rules'] += 1
        
        # Track validation in lineage
        validation_event_id = self.lineage_tracker.track_validation(
            source_event_id, validation_rules, validation_results
        )
        
        return validation_results, validation_event_id
    
    def _apply_validation_rule(self, df, rule: Dict) -> Dict:
        """Apply a single validation rule"""
        rule_type = rule.get('type')
        column = rule.get('column')
        rule_name = rule.get('name', f"{rule_type}_{column}")
        
        try:
            if rule_type == 'not_null':
                violations = df.filter(col(column).isNull()).count()
                passed = violations == 0
                
            elif rule_type == 'unique':
                total_rows = df.count()
                unique_rows = df.select(column).distinct().count()
                violations = total_rows - unique_rows
                passed = violations == 0
                
            elif rule_type == 'range':
                min_val = rule.get('min_value')
                max_val = rule.get('max_value')
                violations = df.filter(
                    (col(column) < min_val) | (col(column) > max_val)
                ).count()
                passed = violations == 0
                
            elif rule_type == 'format':
                pattern = rule.get('pattern')
                violations = df.filter(~col(column).rlike(pattern)).count()
                passed = violations == 0
                
            elif rule_type == 'custom':
                condition = rule.get('condition')
                violations = df.filter(~expr(condition)).count()
                passed = violations == 0
                
            else:
                return {
                    'rule_name': rule_name,
                    'rule_type': rule_type,
                    'column': column,
                    'passed': False,
                    'violations': 0,
                    'error': f"Unknown rule type: {rule_type}"
                }
            
            return {
                'rule_name': rule_name,
                'rule_type': rule_type,
                'column': column,
                'passed': passed,
                'violations': violations,
                'error': None
            }
            
        except Exception as e:
            self.logger.error(f"Error applying rule {rule_name}: {str(e)}")
            return {
                'rule_name': rule_name,
                'rule_type': rule_type,
                'column': column,
                'passed': False,
                'violations': 0,
                'error': str(e)
            }

class ReconciliationEngine:
    """Handles data reconciliation between datasets"""
    
    def __init__(self, spark_session: SparkSession, lineage_tracker: DataLineageTracker):
        self.spark = spark_session
        self.lineage_tracker = lineage_tracker
        self.logger = logging.getLogger(__name__)
    
    def reconcile_datasets(self, df1, df2, join_keys: List[str], 
                          compare_columns: List[str], 
                          source1_event_id: str, source2_event_id: str):
        """Reconcile two datasets"""
        
        # Remove any existing _source columns to avoid conflicts
        if "_source" in df1.columns:
            df1 = df1.drop("_source")
        if "_source" in df2.columns:
            df2 = df2.drop("_source")
        
        # Prepare dataframes with source indicators and handle column name conflicts
        # For join keys, keep them as-is. For other columns, prefix to avoid conflicts
        def prepare_columns(df, prefix, join_keys):
            cols = []
            for c in df.columns:
                if c in join_keys:
                    # Keep join keys as-is for the join
                    cols.append(col(c))
                else:
                    # Prefix other columns to avoid conflicts
                    cols.append(col(c).alias(f"{prefix}_{c}"))
            return cols
        
        df1_cols = prepare_columns(df1, "bank", join_keys)
        df2_cols = prepare_columns(df2, "gl", join_keys)
        
        df1_prepared = df1.select(*df1_cols).withColumn("_bank_source", lit("bank"))
        df2_prepared = df2.select(*df2_cols).withColumn("_gl_source", lit("gl"))
        
        # Full outer join on key columns
        join_condition = [df1_prepared[key] == df2_prepared[key] for key in join_keys]
        joined_df = df1_prepared.alias("s1").join(
            df2_prepared.alias("s2"), 
            join_condition, 
            "full_outer"
        )
        
        # Select specific columns to avoid duplicates - keep join keys from left side only
        columns_to_select = []
        
        # Add all columns from left side (s1)
        for col_name in df1_prepared.columns:
            columns_to_select.append(f"s1.{col_name}")
            
        # Add columns from right side (s2) except join keys
        for col_name in df2_prepared.columns:
            if col_name not in join_keys:
                columns_to_select.append(f"s2.{col_name}")
        
        # Select the specific columns to create clean DataFrame
        joined_df = joined_df.select(*[col(c) for c in columns_to_select])
        
        # Identify matches, missing in source1, missing in source2
        matches = joined_df.filter(
            col("_bank_source").isNotNull() & col("_gl_source").isNotNull()
        )
        
        missing_in_s1 = joined_df.filter(
            col("_bank_source").isNull() & col("_gl_source").isNotNull()
        )
        
        missing_in_s2 = joined_df.filter(
            col("_bank_source").isNotNull() & col("_gl_source").isNull()
        )
        
        # Check for discrepancies in matching records
        discrepancies = None
        if compare_columns and matches.count() > 0:
            discrepancy_conditions = []
            for col_spec in compare_columns:
                # Handle column mapping format: "col1:col2" or just "col1"
                if ':' in col_spec:
                    col1_name, col2_name = col_spec.split(':')
                else:
                    col1_name = col2_name = col_spec
                
                # Handle prefixed column names
                bank_col = f"bank_{col1_name}" if f"bank_{col1_name}" in [c for c in matches.columns] else col1_name
                gl_col = f"gl_{col2_name}" if f"gl_{col2_name}" in [c for c in matches.columns] else col2_name
                
                discrepancy_conditions.append(
                    col(bank_col) != col(gl_col)
                )
            
            if discrepancy_conditions:
                discrepancies = matches.filter(
                    reduce(lambda a, b: a | b, discrepancy_conditions)
                )
        
        # Calculate reconciliation results
        match_results = {
            'total_records_source1': df1.count(),
            'total_records_source2': df2.count(),
            'matched_records': matches.count() if matches else 0,
            'missing_in_source1': missing_in_s1.count() if missing_in_s1 else 0,
            'missing_in_source2': missing_in_s2.count() if missing_in_s2 else 0,
            'discrepancies': discrepancies.count() if discrepancies else 0,
            'match_rate': 0.0
        }
        
        # Calculate match rate
        total_unique = match_results['total_records_source1'] + match_results['missing_in_source1']
        if total_unique > 0:
            match_results['match_rate'] = match_results['matched_records'] / total_unique
        
        # Track reconciliation in lineage
        reconciliation_event_id = self.lineage_tracker.track_reconciliation(
            source1_event_id, source2_event_id, "full_reconciliation", match_results
        )
        
        return {
            'results': match_results,
            'matched_df': matches,
            'missing_in_source1_df': missing_in_s1,
            'missing_in_source2_df': missing_in_s2,
            'discrepancies_df': discrepancies,
            'reconciliation_event_id': reconciliation_event_id
        }

class DataPipeline:
    """Main data pipeline orchestrator"""
    
    def __init__(self):
        self.spark = self._initialize_spark()
        self.lineage_tracker = DataLineageTracker(self.spark)
        self.validation_engine = ValidationEngine(self.spark, self.lineage_tracker)
        self.reconciliation_engine = ReconciliationEngine(self.spark, self.lineage_tracker)
        self.logger = logging.getLogger(__name__)
    
    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with required configurations"""
        return SparkSession.builder \
            .appName("BankReconciliationPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def load_data(self, file_path: str, file_type: str = "auto") -> tuple:
        """Load data from file and track in lineage"""
        
        if file_type == "auto":
            if file_path.endswith('.csv'):
                file_type = "csv"
            elif file_path.endswith(('.xlsx', '.xls')):
                file_type = "excel"
            else:
                raise ValueError(f"Unsupported file type for {file_path}")
        
        try:
            if file_type == "csv":
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
            elif file_type == "excel":
                # Read Excel file using pandas first, then convert to Spark DataFrame
                try:
                    # Try with openpyxl engine first
                    pandas_df = pd.read_excel(file_path, engine='openpyxl')
                except Exception as e1:
                    try:
                        # Fallback to default engine
                        pandas_df = pd.read_excel(file_path)
                    except Exception as e2:
                        self.logger.error(f"Failed to read Excel file with both engines: {e1}, {e2}")
                        raise Exception(f"Could not read Excel file: {e2}")
                
                # Convert pandas DataFrame to Spark DataFrame
                try:
                    df = self.spark.createDataFrame(pandas_df)
                except Exception as e:
                    # Handle potential schema issues
                    self.logger.warning(f"Schema conversion issue, converting to string: {e}")
                    # Convert all columns to string to avoid type issues
                    pandas_df = pandas_df.astype(str)
                    df = self.spark.createDataFrame(pandas_df)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            # Track source in lineage
            source_event_id = self.lineage_tracker.track_source(
                file_path, file_type, len(df.columns), df.count()
            )
            
            return df, source_event_id
            
        except Exception as e:
            self.logger.error(f"Error loading data from {file_path}: {str(e)}")
            raise
    
    def run_validation(self, df, validation_rules: List[Dict], source_event_id: str):
        """Run validation on dataset"""
        return self.validation_engine.validate_data(df, validation_rules, source_event_id)
    
    def run_reconciliation(self, df1, df2, join_keys: List[str], 
                          compare_columns: List[str], 
                          source1_event_id: str, source2_event_id: str):
        """Run reconciliation between datasets"""
        return self.reconciliation_engine.reconcile_datasets(
            df1, df2, join_keys, compare_columns, source1_event_id, source2_event_id
        )
    
    def get_lineage_logs(self):
        """Get lineage tracking logs"""
        return self.lineage_tracker.lineage_events
    
    def export_results(self, df, output_path: str, format: str = "parquet"):
        """Export results to file"""
        if format == "parquet":
            df.write.mode("overwrite").parquet(output_path)
        elif format == "csv":
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        elif format == "json":
            df.write.mode("overwrite").json(output_path)
        
        self.logger.info(f"Results exported to {output_path}")
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
