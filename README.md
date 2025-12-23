
## Project Overview

This repository contains a complete data engineering pipeline implementation demonstrating **Medallion Architecture** (Bronze → Silver → Gold) using **PySpark**. The pipeline processes ServiceNow incident ticket data through three layers of increasing data quality and business value.

## Project Objective

Build a production-quality data pipeline that:
- Ingests raw CSV data (Bronze Layer)
- Cleans and transforms data (Silver Layer)
- Aggregates data for analytics (Gold Layer)

## Architecture

```
Raw CSV → Bronze (Parquet) → Silver (Parquet) → Gold (CSV)
           ↓                  ↓                  ↓
       Raw Data          Cleaned Data      Business Metrics
```

## Quick Start

### Prerequisites
- Python 3.8+
- Java 11 (required for PySpark)

### Installation

```bash
# Clone the repository
git clone https://github.com/HarshaArepalli/TakeHome_BS.git
cd TakeHome_BS/data-engineering-pipeline

# Install dependencies
pip3 install -r requirements.txt

# Place your CSV file
cp path/to/servicenow_incidents_10k.csv data/raw/

# Run the pipeline
python3 src/main.py
```

## Project Structure


TakeHome_BS/
├── data-engineering-pipeline/     # Main pipeline project
│   ├── src/                      # Source code
│   │   ├── bronze/              # Bronze layer (ingestion)
│   │   ├── silver/              # Silver layer (transformation)
│   │   ├── gold/                # Gold layer (aggregation)
│   │   ├── utils/               # Utilities
│   │   └── main.py              # Pipeline orchestrator
│   ├── tests/                   # Unit tests
│   ├── data/                    # Data storage
│   ├── config/                  # Configuration files
│   ├── notebooks/               # Jupyter notebooks
│   └── requirements.txt         # Python dependencies
└── .github/workflows/           # CI/CD workflows


## Technologies

- **PySpark 3.5.0**: Distributed data processing
- **Python 3.8+**: Programming language
- **Parquet**: Columnar storage format
- **pytest**: Unit testing
- **GitHub Actions**: CI/CD automation

## Features Implemented

### Core Requirements
- Bronze Layer: Raw data ingestion
- Silver Layer: Data cleaning & transformation
- Gold Layer: Business aggregations
- Comprehensive README with design decisions
- Hosted on GitHub

### Bonus Features
-  Unit tests with pytest
-  GitHub Actions CI/CD pipeline
-  Code linting (pylint, black)
-  Configuration management
-  Comprehensive documentation

## Pipeline Layers

###  Bronze Layer
- Ingests raw CSV data
- Preserves original schema
- Saves as Parquet format

###  Silver Layer
- Normalizes column names
- Removes invalid records
- Converts data types
- Standardizes formatting

###  Gold Layer
- Aggregates by state & priority
- Generates analytics-ready data
- Exports to CSV

##  Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific layer tests
pytest tests/test_bronze.py -v
pytest tests/test_silver.py -v
pytest tests/test_gold.py -v

# Validate pipeline structure
python3 tests/validate_pipeline.py
```

##  Documentation

- [Main README](data-engineering-pipeline/README.md) - Detailed pipeline documentation
- [Java Installation Guide](data-engineering-pipeline/JAVA_INSTALLATION.md) - Setup instructions
- [Configuration Guide](data-engineering-pipeline/config/pipeline_config.yaml) - Pipeline settings

##  Design Decisions

1. **PySpark over Pandas**: Scalability and production readiness
2. **Parquet format**: Efficient columnar storage for analytics
3. **Modular architecture**: Separation of concerns for maintainability
4. **Configuration-driven**: Easy environment switching
5. **Comprehensive testing**: Ensures code quality and reliability

##  CI/CD Pipeline

The project includes a GitHub Actions workflow that:
- Runs on every push and pull request
- Lints code with pylint and black
- Executes unit tests
- Validates pipeline structure

##  Sample Results

```
+------------+--------+-------------+
|state       |priority|ticket_count |
+------------+--------+-------------+
|Closed      |2       |1            |
|In Progress |3       |2            |
|Resolved    |3       |1            |
+------------+--------+-------------+
```
