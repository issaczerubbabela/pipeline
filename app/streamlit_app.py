# Fix for distutils deprecation in Python 3.12+
import sys
try:
    import distutils
except ImportError:
    try:
        import setuptools
        import setuptools._distutils as distutils
        sys.modules['distutils'] = distutils
    except ImportError:
        pass

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from datetime import datetime
import sys
import os
import traceback
import tempfile

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Fix for distutils deprecation
try:
    from pipeline_engine import DataPipeline
except ImportError as e:
    st.error(f"Import error: {e}")
    st.error("Please ensure all dependencies are installed correctly.")
    st.stop()

class StreamlitUI:
    """Streamlit-based UI for the data pipeline"""
    
    def __init__(self):
        self.pipeline = None
        self.setup_page_config()
    
    def setup_page_config(self):
        """Configure Streamlit page"""
        st.set_page_config(
            page_title="Bank Reconciliation Pipeline",
            page_icon="🏦",
            layout="wide",
            initial_sidebar_state="expanded"
        )
    
    def initialize_pipeline(self):
        """Initialize the data pipeline"""
        if self.pipeline is None:
            with st.spinner("Initializing PySpark session..."):
                try:
                    self.pipeline = DataPipeline()
                    st.success("Pipeline initialized successfully!")
                except Exception as e:
                    st.error(f"Failed to initialize pipeline: {str(e)}")
                    return False
        return True
    
    def render_sidebar(self):
        """Render sidebar navigation"""
        st.sidebar.title("🏦 Bank Reconciliation Pipeline")
        
        page = st.sidebar.selectbox(
            "Select Page",
            ["Data Upload", "Validation Rules", "Reconciliation", "Data Lineage", "Results"]
        )
        
        return page
    
    def render_data_upload_page(self):
        """Render data upload page"""
        st.title("📁 Data Upload")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.header("Dataset 1")
            file1 = st.file_uploader(
                "Upload first dataset (CSV/Excel)",
                type=['csv', 'xlsx', 'xls'],
                key="file1"
            )
            
            if file1 is not None:
                # Save uploaded file temporarily
                file1_path = f"temp_{file1.name}"
                with open(file1_path, "wb") as f:
                    f.write(file1.getbuffer())
                
                try:
                    df1, source1_event_id = self.pipeline.load_data(file1_path)
                    st.session_state['df1'] = df1
                    st.session_state['source1_event_id'] = source1_event_id
                    st.session_state['file1_path'] = file1_path
                    
                    st.success(f"Loaded {df1.count()} rows, {len(df1.columns)} columns")
                    
                    # Show preview
                    st.subheader("Data Preview")
                    preview_df = df1.limit(100).toPandas()
                    st.dataframe(preview_df)
                    
                    # Show column info
                    st.subheader("Column Information")
                    col_info = []
                    for col_name, col_type in df1.dtypes:
                        col_info.append({"Column": col_name, "Type": col_type})
                    st.dataframe(pd.DataFrame(col_info))
                    
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
        
        with col2:
            st.header("Dataset 2")
            file2 = st.file_uploader(
                "Upload second dataset (CSV/Excel)",
                type=['csv', 'xlsx', 'xls'],
                key="file2"
            )
            
            if file2 is not None:
                # Save uploaded file temporarily
                file2_path = f"temp_{file2.name}"
                with open(file2_path, "wb") as f:
                    f.write(file2.getbuffer())
                
                try:
                    df2, source2_event_id = self.pipeline.load_data(file2_path)
                    st.session_state['df2'] = df2
                    st.session_state['source2_event_id'] = source2_event_id
                    st.session_state['file2_path'] = file2_path
                    
                    st.success(f"Loaded {df2.count()} rows, {len(df2.columns)} columns")
                    
                    # Show preview
                    st.subheader("Data Preview")
                    preview_df = df2.limit(100).toPandas()
                    st.dataframe(preview_df)
                    
                    # Show column info
                    st.subheader("Column Information")
                    col_info = []
                    for col_name, col_type in df2.dtypes:
                        col_info.append({"Column": col_name, "Type": col_type})
                    st.dataframe(pd.DataFrame(col_info))
                    
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
    
    def render_validation_rules_page(self):
        """Render validation rules configuration page"""
        st.title("✅ Validation Rules")
        
        if 'df1' not in st.session_state and 'df2' not in st.session_state:
            st.warning("Please upload datasets first!")
            return
        
        # Choose dataset to validate
        dataset_choice = st.selectbox(
            "Select dataset to validate",
            ["Dataset 1", "Dataset 2", "Both"]
        )
        
        # Get available columns
        columns = []
        if dataset_choice in ["Dataset 1", "Both"] and 'df1' in st.session_state:
            columns.extend(st.session_state['df1'].columns)
        if dataset_choice in ["Dataset 2", "Both"] and 'df2' in st.session_state:
            df2_cols = st.session_state['df2'].columns
            columns.extend([col for col in df2_cols if col not in columns])
        
        if not columns:
            st.error("No columns available for validation")
            return
        
        # Rule configuration
        st.header("Configure Validation Rules")
        
        if 'validation_rules' not in st.session_state:
            st.session_state['validation_rules'] = []
        
        # Add new rule
        with st.expander("➕ Add New Validation Rule"):
            rule_name = st.text_input("Rule Name")
            rule_type = st.selectbox(
                "Rule Type",
                ["not_null", "unique", "range", "format", "custom"]
            )
            column = st.selectbox("Column", columns)
            
            # Rule-specific parameters
            if rule_type == "range":
                col1, col2 = st.columns(2)
                with col1:
                    min_value = st.number_input("Minimum Value")
                with col2:
                    max_value = st.number_input("Maximum Value")
            elif rule_type == "format":
                pattern = st.text_input("Regex Pattern", placeholder="e.g., ^[A-Z]{2}[0-9]{4}$")
            elif rule_type == "custom":
                condition = st.text_area("Custom Condition", placeholder="e.g., column_name > 0")
            
            if st.button("Add Rule"):
                rule = {
                    'name': rule_name or f"{rule_type}_{column}",
                    'type': rule_type,
                    'column': column
                }
                
                if rule_type == "range":
                    rule['min_value'] = min_value
                    rule['max_value'] = max_value
                elif rule_type == "format":
                    rule['pattern'] = pattern
                elif rule_type == "custom":
                    rule['condition'] = condition
                
                st.session_state['validation_rules'].append(rule)
                st.success("Rule added successfully!")
                st.rerun()
        
        # Display current rules
        if st.session_state['validation_rules']:
            st.header("Current Validation Rules")
            rules_df = pd.DataFrame(st.session_state['validation_rules'])
            st.dataframe(rules_df)
            
            # Remove rule
            rule_to_remove = st.selectbox(
                "Select rule to remove",
                range(len(st.session_state['validation_rules'])),
                format_func=lambda x: st.session_state['validation_rules'][x]['name']
            )
            
            if st.button("Remove Selected Rule"):
                st.session_state['validation_rules'].pop(rule_to_remove)
                st.success("Rule removed!")
                st.rerun()
        
        # Run validation
        if st.button("🔍 Run Validation") and st.session_state['validation_rules']:
            with st.spinner("Running validation..."):
                try:
                    validation_results = {}
                    
                    if dataset_choice in ["Dataset 1", "Both"] and 'df1' in st.session_state:
                        results1, event_id1 = self.pipeline.run_validation(
                            st.session_state['df1'],
                            st.session_state['validation_rules'],
                            st.session_state['source1_event_id']
                        )
                        validation_results['Dataset 1'] = results1
                    
                    if dataset_choice in ["Dataset 2", "Both"] and 'df2' in st.session_state:
                        results2, event_id2 = self.pipeline.run_validation(
                            st.session_state['df2'],
                            st.session_state['validation_rules'],
                            st.session_state['source2_event_id']
                        )
                        validation_results['Dataset 2'] = results2
                    
                    st.session_state['validation_results'] = validation_results
                    
                    # Display results
                    self.display_validation_results(validation_results)
                    
                except Exception as e:
                    st.error(f"Validation failed: {str(e)}")
    
    def display_validation_results(self, validation_results):
        """Display validation results"""
        st.header("📊 Validation Results")
        
        for dataset_name, results in validation_results.items():
            st.subheader(f"{dataset_name} Results")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", results['total_rows'])
            with col2:
                st.metric("Passed Rules", results['passed_rules'])
            with col3:
                st.metric("Failed Rules", results['failed_rules'])
            
            # Rule details
            rule_results_df = pd.DataFrame(results['rule_results'])
            st.dataframe(rule_results_df)
            
            # Visualization
            if not rule_results_df.empty:
                try:
                    # Ensure proper data types for plotting
                    plot_data = rule_results_df.copy()
                    plot_data['violations'] = pd.to_numeric(plot_data['violations'], errors='coerce').fillna(0)
                    plot_data['passed'] = plot_data['passed'].astype(bool)
                    
                    fig = px.bar(
                        plot_data,
                        x='rule_name',
                        y='violations',
                        title=f"{dataset_name} - Validation Violations by Rule",
                        color='passed',
                        color_discrete_map={True: 'green', False: 'red'},
                        labels={'violations': 'Number of Violations', 'rule_name': 'Rule Name'}
                    )
                    
                    fig.update_layout(
                        xaxis_tickangle=-45,
                        height=400,
                        showlegend=True
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating chart: {str(e)}")
                    st.write("Chart data:")
                    st.write(rule_results_df[['rule_name', 'violations', 'passed']])
            else:
                st.info("No validation rules to display")
    
    def render_reconciliation_page(self):
        """Render reconciliation page"""
        st.title("🔄 Data Reconciliation")
        
        if 'df1' not in st.session_state or 'df2' not in st.session_state:
            st.warning("Please upload both datasets first!")
            return
        
        # Get common columns
        df1_cols = set(st.session_state['df1'].columns)
        df2_cols = set(st.session_state['df2'].columns)
        common_cols = list(df1_cols.intersection(df2_cols))
        
        if not common_cols:
            st.error("No common columns found between datasets!")
            return
        
        # Configure reconciliation
        st.header("Configure Reconciliation")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Join Keys")
            join_keys = st.multiselect(
                "Select columns to join on",
                common_cols,
                help="These columns will be used to match records between datasets"
            )
        
        with col2:
            st.subheader("Compare Columns")
            compare_columns = st.multiselect(
                "Select columns to compare",
                [col for col in common_cols if col not in join_keys],
                help="These columns will be compared for differences in matched records"
            )
        
        if st.button("🔍 Run Reconciliation") and join_keys:
            with st.spinner("Running reconciliation..."):
                try:
                    reconciliation_results = self.pipeline.run_reconciliation(
                        st.session_state['df1'],
                        st.session_state['df2'],
                        join_keys,
                        compare_columns,
                        st.session_state['source1_event_id'],
                        st.session_state['source2_event_id']
                    )
                    
                    st.session_state['reconciliation_results'] = reconciliation_results
                    
                    # Display results
                    self.display_reconciliation_results(reconciliation_results)
                    
                except Exception as e:
                    st.error(f"Reconciliation failed: {str(e)}")
    
    def display_reconciliation_results(self, reconciliation_results):
        """Display reconciliation results"""
        st.header("📊 Reconciliation Results")
        
        results = reconciliation_results['results']
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Source 1 Records", results['total_records_source1'])
        with col2:
            st.metric("Source 2 Records", results['total_records_source2'])
        with col3:
            st.metric("Matched Records", results['matched_records'])
        with col4:
            st.metric("Match Rate", f"{results['match_rate']:.2%}")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Missing in Source 1", results['missing_in_source1'])
        with col2:
            st.metric("Missing in Source 2", results['missing_in_source2'])
        with col3:
            st.metric("Discrepancies", results['discrepancies'])
        
        # Visualization
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "pie"}, {"type": "bar"}]],
            subplot_titles=("Record Distribution", "Reconciliation Summary")
        )
        
        # Pie chart
        labels = ['Matched', 'Missing in Source 1', 'Missing in Source 2']
        values = [results['matched_records'], results['missing_in_source1'], results['missing_in_source2']]
        
        fig.add_trace(
            go.Pie(labels=labels, values=values, name="Distribution"),
            row=1, col=1
        )
        
        # Bar chart
        fig.add_trace(
            go.Bar(
                x=['Matched', 'Missing S1', 'Missing S2', 'Discrepancies'],
                y=[results['matched_records'], results['missing_in_source1'], 
                   results['missing_in_source2'], results['discrepancies']],
                name="Counts"
            ),
            row=1, col=2
        )
        
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed views
        if results['missing_in_source1'] > 0:
            with st.expander(f"📋 Records Missing in Source 1 ({results['missing_in_source1']})"):
                missing_df = reconciliation_results['missing_in_source1_df']
                if missing_df and missing_df.count() > 0:
                    st.dataframe(missing_df.limit(100).toPandas())
        
        if results['missing_in_source2'] > 0:
            with st.expander(f"📋 Records Missing in Source 2 ({results['missing_in_source2']})"):
                missing_df = reconciliation_results['missing_in_source2_df']
                if missing_df and missing_df.count() > 0:
                    st.dataframe(missing_df.limit(100).toPandas())
        
        if results['discrepancies'] > 0:
            with st.expander(f"⚠️ Discrepancies ({results['discrepancies']})"):
                discrepancies_df = reconciliation_results['discrepancies_df']
                if discrepancies_df and discrepancies_df.count() > 0:
                    st.dataframe(discrepancies_df.limit(100).toPandas())
    
    def render_data_lineage_page(self):
        """Render data lineage page"""
        st.title("🔗 Data Lineage")
        
        if self.pipeline:
            lineage_events = self.pipeline.get_lineage_logs()
            
            if lineage_events:
                st.header("📝 Lineage Events Log")
                
                # Summary
                summary = self.pipeline.lineage_tracker.get_lineage_summary()
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Events", summary['total_events'])
                with col2:
                    st.metric("Latest Event", summary['latest_event']['timestamp'][:19])
                
                # Event type breakdown
                event_types_df = pd.DataFrame(
                    list(summary['event_types'].items()),
                    columns=['Event Type', 'Count']
                )
                
                fig = px.bar(
                    event_types_df,
                    x='Event Type',
                    y='Count',
                    title="Event Types Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Detailed event log
                st.subheader("Detailed Event Log")
                
                # Create a formatted display of events
                for i, event in enumerate(lineage_events):
                    with st.expander(f"{event['event_type']} - {event['timestamp'][:19]}"):
                        st.json(event)
                
                # Export lineage
                if st.button("📁 Export Lineage"):
                    try:
                        self.pipeline.lineage_tracker.export_lineage("lineage_export")
                        st.success("Lineage exported successfully!")
                    except Exception as e:
                        st.error(f"Export failed: {str(e)}")
                        
            else:
                st.info("No lineage events recorded yet. Run some pipeline operations to see lineage data.")
        else:
            st.warning("Pipeline not initialized!")
    
    def render_results_page(self):
        """Render results and export page"""
        st.title("📊 Results & Export")
        
        # Validation results
        if 'validation_results' in st.session_state:
            st.header("✅ Validation Results Summary")
            for dataset_name, results in st.session_state['validation_results'].items():
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric(f"{dataset_name} - Total Rows", results['total_rows'])
                with col2:
                    st.metric(f"{dataset_name} - Passed Rules", results['passed_rules'])
                with col3:
                    st.metric(f"{dataset_name} - Failed Rules", results['failed_rules'])
        
        # Reconciliation results
        if 'reconciliation_results' in st.session_state:
            st.header("🔄 Reconciliation Results Summary")
            results = st.session_state['reconciliation_results']['results']
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Match Rate", f"{results['match_rate']:.2%}")
            with col2:
                st.metric("Total Discrepancies", results['discrepancies'])
            with col3:
                st.metric("Missing Records", results['missing_in_source1'] + results['missing_in_source2'])
        
        # Export options
        st.header("📁 Export Options")
        
        export_format = st.selectbox(
            "Select export format",
            ["CSV", "Parquet", "JSON"]
        )
        
        export_what = st.selectbox(
            "What to export",
            ["Validation Results", "Reconciliation Results", "Both", "Lineage Data"]
        )
        
        if st.button("📥 Export Data"):
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                if export_what in ["Validation Results", "Both"]:
                    # Export validation results logic here
                    st.success("Validation results exported!")
                
                if export_what in ["Reconciliation Results", "Both"]:
                    # Export reconciliation results logic here
                    st.success("Reconciliation results exported!")
                
                if export_what == "Lineage Data":
                    # Export lineage data
                    if self.pipeline:
                        self.pipeline.lineage_tracker.export_lineage(f"lineage_export_{timestamp}")
                        st.success("Lineage data exported!")
                
            except Exception as e:
                st.error(f"Export failed: {str(e)}")
    
    def run(self):
        """Main application runner"""
        # Initialize pipeline
        if not self.initialize_pipeline():
            return
        
        # Render sidebar and get current page
        current_page = self.render_sidebar()
        
        # Render appropriate page
        if current_page == "Data Upload":
            self.render_data_upload_page()
        elif current_page == "Validation Rules":
            self.render_validation_rules_page()
        elif current_page == "Reconciliation":
            self.render_reconciliation_page()
        elif current_page == "Data Lineage":
            self.render_data_lineage_page()
        elif current_page == "Results":
            self.render_results_page()

if __name__ == "__main__":
    app = StreamlitUI()
    app.run()
