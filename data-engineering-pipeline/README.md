# Data Engineering Pipeline - ServiceNow Incident Data

A medallion architecture (Bronze → Silver → Gold) data pipeline built with PySpark to process IT incident ticket data.

## 📋 Overview

This project implements a three-layer data pipeline that ingests, cleanses, and aggregates ServiceNow incident data:

- **Bronze Layer**: Raw data ingestion from CSV
- **Silver Layer**: Data cleaning, transformation, and standardization
- **Gold Layer**: Business-level aggregations for analytics

## 🏗️ Architecture

```
data-engineering-pipeline/
├── data/
│   ├── raw/              # Source CSV files
│   ├── bronze/           # Raw ingested data (Parquet)
│   ├── silver/           # Cleaned & transformed data (Parquet)
│   └── gold/             # Aggregated analytics data (CSV)
├── src/
│   ├── bronze/           # Bronze layer ingestion logic
│   ├── silver/           # Silver layer transformation logic
│   ├── gold/             # Gold layer aggregation logic
│   ├── utils/            # Shared utilities (Spark session, schemas)
│   └── main.py           # Pipeline orchestrator
├── tests/                # Unit tests
├── config/               # Configuration files
├── notebooks/            # Jupyter notebooks for exploration
└── requirements.txt      # Python dependencies
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Java 8 or 11 (required for PySpark)

### Installation

1. **Clone this repository**:
```bash
git clone https://github.com/HarshaArepalli/TakeHome_BS.git
cd TakeHome_BS/data-engineering-pipeline
```

2. **Install dependencies**:
```bash
pip3 install -r requirements.txt
```

3. **Place the CSV file**:
```bash
# Copy the CSV to the raw data directory
cp ~/Downloads/servicenow_incidents_10k\ \(1\).csv data/raw/servicenow_incidents_10k.csv
```

### Running the Pipeline

Execute the full pipeline:
```bash
cd data-engineering-pipeline
python3 src/main.py
```

The pipeline will:
1. Ingest raw CSV data → Bronze layer (Parquet)
2. Clean and transform → Silver layer (Parquet)
3. Aggregate for analytics → Gold layer (CSV)

## 📊 Pipeline Layers

### Bronze Layer
**Purpose**: Raw data ingestion with no transformations

**Actions**:
- Read CSV file with schema inference
- Preserve all original data
- Save as Parquet format for efficient storage

**Key Features**:
- Schema preservation
- Data lineage tracking
- Allows for reprocessing

**Output**: `data/bronze/incidents/`

### Silver Layer
**Purpose**: Data cleaning and standardization

**Actions**:
- Normalize column names to snake_case
- Remove invalid records (missing `number` or `sys_id`)
- Convert date strings to timestamp format (`M/d/yyyy H:mm`)
- Standardize text fields (trim whitespace)
- Convert string booleans to actual booleans (`sla_breached`, `knowledge_linked`)
- Cast numeric fields to proper types (`priority`, `impact`, `urgency`, `reopen_count`)

**Key Features**:
- Data quality enforcement
- Type safety
- Consistent formatting

**Output**: `data/silver/incidents/`

### Gold Layer
**Purpose**: Business-level aggregations

**Actions**:
- Aggregate ticket counts by status and priority
- Generate analytics-ready datasets
- Save as CSV for easy BI tool consumption

**Key Features**:
- Query-optimized
- Business logic applied
- Human-readable format

**Output**: `data/gold/ticket_summary/`

## 🎯 Design Decisions

### 1. **Parquet for Bronze/Silver Layers**
- **Columnar storage format**: Better compression and query performance
- **Schema evolution support**: Can add new fields without breaking pipeline
- **Efficient compression**: Reduces storage costs
- **Fast query performance**: Columnar format optimized for analytics

### 2. **CSV for Gold Layer**
- **Human-readable format**: Easy to inspect and validate
- **Easy consumption by BI tools**: Direct import into Excel, Tableau, etc.
- **Small aggregated dataset size**: Minimal overhead

### 3. **PySpark Over Pandas**
- **Scalability**: Can handle datasets from MB to TB without code changes
- **Distributed processing**: Leverages cluster computing when available
- **Production-ready**: Same code works locally and on Databricks/EMR
- **Industry standard**: Widely used in enterprise data engineering

### 4. **Modular Architecture**
- **Separation of concerns**: Each layer is independent
- **Easy to test**: Individual components can be unit tested
- **Facilitates parallel development**: Multiple developers can work simultaneously
- **Incremental pipeline runs**: Can rerun specific layers without full pipeline execution

### 5. **Schema Preservation in Bronze**
- **Maintains data lineage**: Can trace transformations back to source
- **Allows for reprocessing**: If business logic changes, can reprocess from bronze
- **Audit trail**: Historical record of raw data

### 6. **Configuration-Driven Design**
- **Path management**: All paths defined in main orchestrator
- **Easy environment switching**: Can easily switch between dev/prod
- **Spark optimization**: Tuned for local execution but scalable

## 🧪 Testing

Run unit tests:
```bash
pytest tests/ -v
```

Test individual layers:
```bash
# Test bronze layer
pytest tests/test_bronze.py -v

# Test silver layer
pytest tests/test_silver.py -v

# Test gold layer
pytest tests/test_gold.py -v
```

## 📈 Sample Output

### Gold Layer - Ticket Summary
```
+------------+--------+-------------+
|state       |priority|ticket_count |
+------------+--------+-------------+
|Closed      |2       |1            |
|Closed      |3       |1            |
|In Progress |3       |2            |
|New         |3       |1            |
|Resolved    |2       |1            |
|Resolved    |3       |1            |
+------------+--------+-------------+
```

## 🔄 Future Enhancements

1. **Delta Lake Integration**: Replace Parquet with Delta for ACID transactions and time travel
2. **Data Quality Checks**: Add Great Expectations for comprehensive validation
3. **Incremental Processing**: Implement change data capture (CDC) for efficient updates
4. **Monitoring & Alerting**: Add logging, metrics, and alerting
5. **Orchestration**: Integrate with Apache Airflow or Databricks workflows
6. **Performance Optimization**: Partitioning strategies for large datasets
7. **Schema Evolution**: Handle schema changes gracefully
8. **CI/CD Pipeline**: Automated testing and deployment

## 📝 Key Technologies

- **PySpark 3.5.0**: Distributed data processing
- **Python 3.8+**: Programming language
- **Parquet**: Columnar storage format
- **pytest**: Unit testing framework

## 🤔 Trade-offs & Considerations

### Why not Pandas?
- Limited to single-machine memory
- Doesn't scale to large datasets
- Not production-ready for big data

### Why Parquet over CSV?
- 10-100x smaller file sizes
- Columnar storage enables faster queries
- Schema is embedded in the file
- Better for analytics workloads

### Why separate layers?
- Allows data quality issues to be fixed without re-ingesting
- Provides audit trail and data lineage
- Enables different consumers at different layers
- Facilitates troubleshooting and debugging

## 📄 Project Structure Details

```
src/
├── __init__.py                    # Makes src a package
├── main.py                        # Pipeline orchestrator
├── bronze/
│   ├── __init__.py
│   └── ingest_raw_data.py        # Raw data ingestion
├── silver/
│   ├── __init__.py
│   └── transform_data.py          # Data cleaning & transformation
├── gold/
│   ├── __init__.py
│   └── aggregate_data.py          # Business aggregations
└── utils/
    ├── __init__.py
    ├── spark_session.py           # Spark session factory
    └── schema.py                  # Schema definitions
```

## 🤝 Contributing

This is a take-home assessment project. For questions or clarifications, please reach out during the interview.

## 📞 Contact

For any questions about this project, please contact during the interview session.

## 📄 License

This project is created for assessment purposes.