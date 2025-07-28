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
            comparison_details = []
            
            for col_spec in compare_columns:
                # Enhanced column mapping format support:
                # - "col1:col2" - exact comparison
                # - "col1:col2:tolerance" - numeric comparison with tolerance
                # - "col1:col2:abs" - absolute value comparison (ignores sign)
                # - "col1:col2:abs:tolerance" - absolute value with tolerance
                # - "col1:col2:opposite" - opposite sign comparison (one positive, one negative)
                # - "col1:col2:opposite:tolerance" - opposite sign with tolerance
                
                parts = col_spec.split(':')
                col1_name = parts[0]
                col2_name = parts[1] if len(parts) >= 2 else parts[0]
                
                # Parse comparison type and tolerance
                comparison_type = "exact"  # default
                tolerance = None
                
                if len(parts) >= 3:
                    if parts[2] in ["abs", "absolute"]:
                        comparison_type = "abs"
                        tolerance = float(parts[3]) if len(parts) >= 4 else None
                    elif parts[2] in ["opposite", "reverse", "inv"]:
                        comparison_type = "opposite"
                        tolerance = float(parts[3]) if len(parts) >= 4 else None
                    else:
                        # Assume it's a tolerance value
                        try:
                            tolerance = float(parts[2])
                            comparison_type = "exact"
                        except ValueError:
                            self.logger.warning(f"Unknown comparison type: {parts[2]}, using exact")
                
                # Handle prefixed column names - improved logic
                available_columns = matches.columns
                self.logger.debug(f"Available columns in matches: {available_columns}")
                
                # Try different column name variations
                possible_bank_cols = [f"bank_{col1_name}", col1_name]
                possible_gl_cols = [f"gl_{col2_name}", col2_name]
                
                bank_col = None
                gl_col = None
                
                # Find the actual column name for bank
                for candidate in possible_bank_cols:
                    if candidate in available_columns:
                        bank_col = candidate
                        break
                
                # Find the actual column name for GL
                for candidate in possible_gl_cols:
                    if candidate in available_columns:
                        gl_col = candidate
                        break
                
                if bank_col is None:
                    self.logger.warning(f"Bank column not found. Tried: {possible_bank_cols}. Available: {available_columns}")
                    continue
                    
                if gl_col is None:
                    self.logger.warning(f"GL column not found. Tried: {possible_gl_cols}. Available: {available_columns}")
                    continue
                
                self.logger.info(f"Comparing {bank_col} vs {gl_col} using {comparison_type}")
                
                # Create comparison condition based on type and tolerance
                if comparison_type == "abs":
                    # Absolute value comparison - ignores sign differences
                    if tolerance is not None:
                        discrepancy_condition = abs(abs(col(bank_col)) - abs(col(gl_col))) > tolerance
                    else:
                        discrepancy_condition = abs(col(bank_col)) != abs(col(gl_col))
                        
                elif comparison_type == "opposite":
                    # Opposite sign comparison - expects one positive, one negative
                    if tolerance is not None:
                        discrepancy_condition = abs(col(bank_col) + col(gl_col)) > tolerance
                    else:
                        discrepancy_condition = col(bank_col) != -col(gl_col)
                        
                else:  # exact comparison
                    if tolerance is not None:
                        discrepancy_condition = abs(col(bank_col) - col(gl_col)) > tolerance
                    else:
                        discrepancy_condition = col(bank_col) != col(gl_col)
                
                discrepancy_conditions.append(discrepancy_condition)
                comparison_details.append({
                    'bank_column': bank_col,
                    'gl_column': gl_col,
                    'comparison_type': comparison_type,
                    'tolerance': tolerance,
                    'mapping': col_spec
                })
            
            if discrepancy_conditions:
                discrepancies = matches.filter(
                    reduce(lambda a, b: a | b, discrepancy_conditions)
                )
                
                # Log comparison details for debugging
                self.logger.info(f"Column comparisons configured: {comparison_details}")
        
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
    
    def create_column_comparison(self, bank_column: str, gl_column: str, 
                               comparison_type: str = "exact", tolerance: float = None) -> str:
        """
        Helper method to create column comparison specifications
        
        Args:
            bank_column: Column name from bank statement
            gl_column: Column name from general ledger
            comparison_type: Type of comparison ("exact", "abs", "opposite")
            tolerance: Optional tolerance for numeric comparisons
            
        Returns:
            String specification for column comparison
        """
        if comparison_type == "abs":
            if tolerance is not None:
                return f"{bank_column}:{gl_column}:abs:{tolerance}"
            else:
                return f"{bank_column}:{gl_column}:abs"
        elif comparison_type == "opposite":
            if tolerance is not None:
                return f"{bank_column}:{gl_column}:opposite:{tolerance}"
            else:
                return f"{bank_column}:{gl_column}:opposite"
        else:  # exact
            if tolerance is not None:
                return f"{bank_column}:{gl_column}:{tolerance}"
            else:
                return f"{bank_column}:{gl_column}"
    
    def get_column_comparison_examples(self) -> dict:
        """
        Get examples of different column comparison formats
        
        Returns:
            Dictionary with examples and descriptions
        """
        return {
            "exact_match_same_name": "amount",
            "exact_match_different_names": "bank_amount:gl_amount", 
            "numeric_with_tolerance": "amount:net_amount:0.01",
            "absolute_value_comparison": "amount:net_amount:abs",
            "absolute_with_tolerance": "amount:net_amount:abs:0.01",
            "opposite_sign_comparison": "amount:net_amount:opposite",
            "opposite_with_tolerance": "amount:net_amount:opposite:0.01",
            "string_comparison": "description:memo",
            "currency_comparison": "bank_currency:gl_currency",
            "multiple_comparisons": [
                "amount:net_amount:abs:0.01",      # Absolute value with tolerance
                "bank_currency:gl_currency",        # Exact match different names
                "bank_description:gl_description"   # Text comparison
            ]
        }

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
