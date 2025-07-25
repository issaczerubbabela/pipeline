import os
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class PipelineConfig:
    """Configuration class for the data pipeline"""
    
    # Spark Configuration
    spark_app_name: str = "BankReconciliationPipeline"
    spark_master: str = "local[*]"
    spark_driver_memory: str = "2g"
    spark_executor_memory: str = "2g"
    
    # File paths
    temp_data_path: str = "temp_data"
    output_path: str = "output"
    lineage_path: str = "lineage"
    
    # Validation settings
    max_validation_errors: int = 1000
    validation_threshold: float = 0.95  # 95% pass rate required
    
    # Reconciliation settings
    match_threshold: float = 0.90  # 90% match rate expected
    
    # UI settings
    max_file_size_mb: int = 100
    preview_rows: int = 100
    
    def __post_init__(self):
        """Create necessary directories"""
        for path in [self.temp_data_path, self.output_path, self.lineage_path]:
            os.makedirs(path, exist_ok=True)
    
    def to_spark_config(self) -> Dict[str, Any]:
        """Convert to Spark configuration dictionary"""
        return {
            "spark.app.name": self.spark_app_name,
            "spark.master": self.spark_master,
            "spark.driver.memory": self.spark_driver_memory,
            "spark.executor.memory": self.spark_executor_memory,
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }

# Default configuration instance
config = PipelineConfig()
