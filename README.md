# Data Engineering Pipeline - TeamCore Technical Test

This repository contains a comprehensive data engineering pipeline solution for processing transaction data, built with Python, Apache Airflow, and SQLite.

## 🏗️ Project Structure

```
data-engineering-pipeline/
├── airflow/                # Apache Airflow DAGs and scripts
│   ├── dags/              # Airflow DAG definitions
│   └── scripts/           # ETL processing scripts
├── config/                # Configuration files
├── data/                  # Data storage (excluded from version control)
│   ├── raw/              # Raw input data
│   ├── processed/        # Cleaned and transformed data
│   └── warehouse/        # Data warehouse files
├── etl/                   # ETL processing modules
├── modeling/              # Data modeling and warehouse logic
├── scripts/               # Utility scripts for data generation
├── sql/                   # SQL queries and analysis
│   └── queries/          # Business logic SQL queries
└── tests/                 # Test suites
```

## 🚀 Features

- **ETL Pipeline**: Complete Extract, Transform, Load pipeline with data validation
- **Data Warehouse**: Dimensional modeling with fact and dimension tables
- **Airflow Integration**: Automated workflow orchestration
- **SQL Analytics**: Business intelligence queries and views
- **Data Quality**: Comprehensive validation and error handling
- **Testing**: Unit and integration tests for all components

## 📊 Data Management

### Important Note about Data Files

This repository follows best practices for data engineering projects:

- **Large data files are excluded** from version control using `.gitignore`
- Sample data can be generated using the provided scripts
- Production data should be stored in appropriate data storage solutions (S3, GCS, etc.)

### Generating Sample Data

To create sample data for testing and development:

```powershell
# Generate sample transaction data
python scripts/generate_transactions.py

# Generate sample log files
python scripts/generate_logs.py
```

## 🛠️ Setup and Installation

### Prerequisites

- Python 3.8+
- Apache Airflow (optional, for workflow orchestration)
- Git

### Installation

1. **Clone the repository**:
   ```powershell
   git clone https://github.com/ulvenforst/Prueba-T-cnica-Data-Engineer-TeamCore.git
   cd data-engineering-pipeline
   ```

2. **Create a virtual environment**:
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

3. **Install dependencies**:
   ```powershell
   pip install -r requirements.txt
   ```

4. **Generate sample data**:
   ```powershell
   python scripts/generate_transactions.py
   python scripts/generate_logs.py
   ```

## 🔄 Usage

### Running the ETL Pipeline

1. **Direct execution**:
   ```powershell
   python main.py
   ```

2. **Individual components**:
   ```powershell
   # Run ETL only
   python etl/run_etl.py
   
   # Build data warehouse
   python modeling/run_warehouse.py
   
   # Execute SQL analysis
   python sql/analysis_runner.py
   ```

### Using Airflow

1. **Start Airflow**:
   ```powershell
   airflow standalone
   ```

2. **Access the web interface**:
   - Open http://localhost:8080
   - Enable the `transactions_dag`

## 📈 SQL Analysis Queries

The project includes several business intelligence queries:

1. **`tarea1_vista_resumen.sql`**: Summary view of transactions
2. **`tarea2_usuarios_multiples_fallas.sql`**: Users with multiple failures
3. **`tarea3_deteccion_anomalias.sql`**: Anomaly detection
4. **`tarea4_indices_triggers.sql`**: Performance optimization

Execute all queries:
```powershell
python sql/analysis_runner.py
```

## 🧪 Testing

Run the test suite:

```powershell
# Run all tests
python -m pytest tests/

# Run specific test files
python -m pytest tests/test_basic.py
python -m pytest tests/test_comprehensive.py
```

## 📁 Data Files and Version Control

### What's Included in Git
- Source code and scripts
- Configuration files
- Documentation
- Test files
- Sample schemas

### What's Excluded from Git
- Large data files (CSV, databases, compressed files)
- Generated outputs
- Temporary files
- Python cache files

### Git LFS Configuration

This project is configured to use Git LFS for large files (if needed):
- `.gitattributes` defines file patterns for LFS
- Large data files should be stored externally in production

## 🔧 Configuration

Configuration settings are managed in `config/settings.py`:
- Database connection strings
- File paths
- Processing parameters
- Logging configuration

## 📝 Development Workflow

1. **Data Generation**: Use scripts to create sample data
2. **Development**: Implement features using the sample data
3. **Testing**: Run tests to ensure functionality
4. **Deployment**: Deploy without data files to production
5. **Production**: Connect to actual data sources

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## 📄 License

This project is part of a technical assessment for TeamCore.

## 📞 Contact

For questions about this implementation, please contact the development team.

---

**Note**: This project demonstrates data engineering best practices including proper version control hygiene, comprehensive testing, and scalable architecture patterns.
