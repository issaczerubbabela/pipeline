# Bank Reconciliation Pipeline

A comprehensive data pipeline solution for bank reconciliation with validation, reconciliation, data lineage tracking, and a user-friendly web interface built with PySpark and Streamlit.

## Features

### 🏦 Core Functionality
- **Data Validation**: Configurable validation rules (not null, unique, range, format, custom)
- **Data Reconciliation**: Advanced reconciliation between bank statements and general ledger
- **Data Lineage**: Complete tracking of data transformations and operations
- **Multi-format Support**: CSV and Excel file support

### 🌐 Web Interface
- **File Upload**: Drag-and-drop interface for CSV/Excel files
- **Rule Configuration**: Interactive rule builder for validation
- **Real-time Results**: Live visualization of validation and reconciliation results
- **Data Lineage Viewer**: Complete audit trail of all operations
- **Export Capabilities**: Multiple output formats (CSV, Parquet, JSON)

### ⚙️ Technical Features
- **PySpark Backend**: Scalable big data processing
- **Streamlit Frontend**: Modern, responsive web interface
- **Data Quality Monitoring**: Comprehensive validation framework
- **Performance Optimized**: Efficient data processing and caching

## Installation

### Prerequisites
- Python 3.8+
- Java 8+ (for PySpark)

### Setup

1. **Clone the repository**
   ```powershell
   git clone <repository-url>
   cd pipeline
   ```

2. **Install dependencies**
   ```powershell
   pip install -r requirements.txt
   ```

3. **Generate sample data (optional)**
   ```powershell
   python -m src.sample_data_generator
   ```

## Usage

### Web Application (Recommended)

Launch the Streamlit web interface:

```powershell
python main.py --mode web
```

The application will open in your browser at `http://localhost:8501`

#### Using the Web Interface:

1. **Data Upload**: Upload your CSV/Excel files
2. **Validation Rules**: Configure validation rules for your data
3. **Reconciliation**: Set up reconciliation parameters
4. **Data Lineage**: View complete audit trail
5. **Results**: Export and analyze results

### Command Line Interface

Run the pipeline in CLI mode:

```powershell
python main.py --mode cli
```

This will:
- Generate sample data
- Run validation on both datasets
- Perform reconciliation
- Export results and lineage

## Project Structure

```
pipeline/
├── app/                         # Web application
│   └── streamlit_app.py         # Streamlit web interface
├── src/                         # Core source code
│   ├── config.py                # Configuration management
│   ├── data_lineage.py         # Data lineage tracking
│   ├── pipeline_engine.py      # Core pipeline engine
│   └── sample_data_generator.py # Sample data generation
├── sample_data/                 # Sample datasets for testing
│   ├── bank_statement.csv       # Sample bank statement
│   ├── general_ledger.csv       # Sample general ledger
│   └── *.xlsx                   # Sample Excel files
├── notebooks/                   # Jupyter notebooks for analysis
│   └── bank_reconciliation_pipeline_demo.ipynb
├── tests/                       # Unit tests
├── scripts/                     # Utility scripts
├── docs/                        # Documentation
├── brs/                         # Business requirements and samples
│   └── 1. Bank Reconciliation Sample.xlsx
├── main.py                      # Application entry point
├── requirements.txt             # Python dependencies
├── .gitignore                   # Git ignore file
└── README.md                    # This file
```

### Archive Folder

Development artifacts and temporary files are stored in the `archive/` folder (excluded from Git):
- `archive/documentation/`: Development notes and installation logs
- `archive/temp_files/`: Temporary data files and logs
- `archive/test_fixes/`: Test fixes and debugging code

## Validation Rules

The system supports various validation rule types:

### Built-in Rules
- **not_null**: Ensures column values are not null
- **unique**: Ensures column values are unique
- **range**: Validates numeric values within specified range
- **format**: Validates string format using regex patterns
- **custom**: Custom validation using SQL expressions

### Example Rules Configuration
```python
validation_rules = [
    {
        'name': 'transaction_id_not_null',
        'type': 'not_null',
        'column': 'transaction_id'
    },
    {
        'name': 'amount_range',
        'type': 'range',
        'column': 'amount',
        'min_value': -10000,
        'max_value': 50000
    },
    {
        'name': 'reference_format',
        'type': 'format',
        'column': 'reference_number',
        'pattern': '^REF[0-9]{6}$'
    }
]
```

## Reconciliation Process

The reconciliation engine compares two datasets and identifies:

- **Matched Records**: Records found in both datasets
- **Missing in Source 1**: Records only in the second dataset
- **Missing in Source 2**: Records only in the first dataset
- **Discrepancies**: Matched records with different values

### Reconciliation Configuration
- **Join Keys**: Columns used to match records between datasets
- **Compare Columns**: Columns compared for discrepancies in matched records

## Data Lineage

The system tracks complete data lineage including:

- **Source Operations**: Data loading and file information
- **Transformation Operations**: Data processing steps
- **Validation Operations**: Rule applications and results
- **Reconciliation Operations**: Matching and comparison results

### Lineage Events
Each operation generates detailed lineage events with:
- Unique event IDs
- Timestamps
- Operation details
- Input/output metadata
- Relationships between events

## Sample Data

The system includes a sample data generator that creates:

- **Bank Statement Data**: Transaction records with realistic patterns
- **General Ledger Data**: Corresponding GL entries with configurable match rates
- **Data Quality Issues**: Intentional issues for testing validation rules

Generate sample data:
```powershell
python -c "from src.sample_data_generator import SampleDataGenerator; SampleDataGenerator().generate_sample_datasets()"
```

## Configuration

Key configuration options in `src/config.py`:

```python
# Spark Configuration
spark_app_name = "BankReconciliationPipeline"
spark_driver_memory = "2g"
spark_executor_memory = "2g"

# Validation settings
max_validation_errors = 1000
validation_threshold = 0.95  # 95% pass rate required

# Reconciliation settings
match_threshold = 0.90  # 90% match rate expected
```

## Performance Considerations

- **File Size**: Recommended maximum 100MB per file for optimal performance
- **Memory**: Adjust Spark memory settings based on data size
- **Partitioning**: Large datasets are automatically partitioned for parallel processing
- **Caching**: Frequently accessed DataFrames are cached for better performance

## Troubleshooting

### Common Issues

1. **Java/Spark Issues**
   - Ensure Java 8+ is installed
   - Check JAVA_HOME environment variable

2. **Memory Issues**
   - Increase Spark driver/executor memory in config
   - Reduce file size or process in chunks

3. **File Format Issues**
   - Ensure CSV files have headers
   - Check Excel file compatibility (xlsx/xls)

### Logs

Application logs are written to:
- Console output
- Log files in `archive/temp_files/` during development

## Future Enhancements

- **Real-time Processing**: Streaming data support
- **Advanced Analytics**: Statistical analysis and anomaly detection
- **Integration**: API endpoints for external system integration
- **Scheduling**: Automated pipeline execution
- **Dashboard**: Executive dashboards and reporting

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions:
- Check the troubleshooting section
- Review the logs
- Create an issue in the repository
