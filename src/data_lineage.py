from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import uuid
from typing import Dict, List, Any

class DataLineageTracker:
    """Tracks data lineage throughout the transformation pipeline"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.lineage_events = []
        self.logger = logging.getLogger(__name__)
        
    def track_source(self, source_path: str, source_type: str, 
                    column_count: int, row_count: int) -> str:
        """Track data source information"""
        event_id = str(uuid.uuid4())
        event = {
            'event_id': event_id,
            'event_type': 'SOURCE',
            'timestamp': datetime.now().isoformat(),
            'source_path': source_path,
            'source_type': source_type,
            'column_count': column_count,
            'row_count': row_count,
            'metadata': {}
        }
        self.lineage_events.append(event)
        self.logger.info(f"Source tracked: {source_path} ({row_count} rows, {column_count} columns)")
        return event_id
    
    def track_transformation(self, source_event_id: str, transformation_type: str,
                           transformation_details: Dict[str, Any],
                           output_row_count: int) -> str:
        """Track transformation operations"""
        event_id = str(uuid.uuid4())
        event = {
            'event_id': event_id,
            'event_type': 'TRANSFORMATION',
            'timestamp': datetime.now().isoformat(),
            'parent_event_id': source_event_id,
            'transformation_type': transformation_type,
            'transformation_details': transformation_details,
            'output_row_count': output_row_count
        }
        self.lineage_events.append(event)
        self.logger.info(f"Transformation tracked: {transformation_type}")
        return event_id
    
    def track_validation(self, source_event_id: str, validation_rules: List[Dict],
                        validation_results: Dict[str, Any]) -> str:
        """Track validation operations"""
        event_id = str(uuid.uuid4())
        event = {
            'event_id': event_id,
            'event_type': 'VALIDATION',
            'timestamp': datetime.now().isoformat(),
            'parent_event_id': source_event_id,
            'validation_rules': validation_rules,
            'validation_results': validation_results
        }
        self.lineage_events.append(event)
        self.logger.info(f"Validation tracked: {len(validation_rules)} rules applied")
        return event_id
    
    def track_reconciliation(self, source1_event_id: str, source2_event_id: str,
                           reconciliation_type: str, match_results: Dict[str, Any]) -> str:
        """Track reconciliation operations"""
        event_id = str(uuid.uuid4())
        event = {
            'event_id': event_id,
            'event_type': 'RECONCILIATION',
            'timestamp': datetime.now().isoformat(),
            'source1_event_id': source1_event_id,
            'source2_event_id': source2_event_id,
            'reconciliation_type': reconciliation_type,
            'match_results': match_results
        }
        self.lineage_events.append(event)
        self.logger.info(f"Reconciliation tracked: {reconciliation_type}")
        return event_id
    
    def get_lineage_df(self):
        """Convert lineage events to Spark DataFrame"""
        if not self.lineage_events:
            return self.spark.createDataFrame([], StructType([]))
        
        return self.spark.createDataFrame(self.lineage_events)
    
    def export_lineage(self, output_path: str):
        """Export lineage to file"""
        lineage_df = self.get_lineage_df()
        if lineage_df.count() > 0:
            lineage_df.write.mode('overwrite').json(output_path)
            self.logger.info(f"Lineage exported to: {output_path}")
    
    def get_lineage_summary(self) -> Dict[str, Any]:
        """Get summary of lineage events"""
        if not self.lineage_events:
            return {"total_events": 0}
        
        summary = {
            "total_events": len(self.lineage_events),
            "event_types": {},
            "latest_event": max(self.lineage_events, key=lambda x: x['timestamp'])
        }
        
        for event in self.lineage_events:
            event_type = event['event_type']
            summary["event_types"][event_type] = summary["event_types"].get(event_type, 0) + 1
        
        return summary
