# Large Scale Data Engineering Project for AI

## Description

A containerized data pipeline using Apache Airflow, PostgreSQL, Hadoop HDFS, and a Streamlit frontend for data visualization. This project implements a complete ETL (Extract, Transform, Load) workflow for processing and analyzing various types of data including air quality, electricity consumption, traffic accidents, and weather data.

## Project Structure

```text
project-root/
│
├── airflow/                             # Airflow service setup and pipelines
│   ├── dags/                            # DAG definitions for data workflows
│   │   ├── a_landing/                   # Raw data ingestion
│   │   ├── b_formatted/                 # Data standardization
│   │   ├── c_trusted/                   # Data quality assurance
│   │   ├── d_explotation/              # View creation for Data Analysis
│   │   ├── e_analysis/                 # Data analysis tasks
│   │   ├── utils/                      # General utilities
│   │   └── mlpipeline.py               # Main pipeline orchestration
│   │
│   ├── plugins/                        # Custom Airflow plugins
│   ├── Dockerfile                      # Airflow service image
│   └── requirements.txt                # Airflow Python dependencies
│
├── notebooks/                          # Jupyter notebooks for analysis
├── plots/                             # Generated visualizations
│
├── docker-compose.yaml               # Project-wide service definitions
├── postgresql-42.7.3.jar             # JDBC driver for PostgreSQL
└── LICENSE                           # Project license

```

## Features

- **Data Ingestion**: Automated collection of data from multiple sources
- **Data Processing**: Standardized ETL pipelines for different data types
- **Quality Assurance**: Data validation and cleaning procedures
- **Data Analysis**: Comprehensive analysis workflows
- **Visualization**: Interactive dashboard using Streamlit
- **Containerization**: Docker-based deployment for all services

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- PostgreSQL
- Apache Airflow
- Hadoop HDFS

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd [repository-name]
```

2. Start the services:
```bash
docker-compose up -d
```

## Usage

1. Access Airflow UI at `http://localhost:8080`
2. Monitor the data pipeline through Airflow's interface

## Data Pipeline

The project implements a multi-stage data pipeline:

1. **Landing Zone**: Raw data ingestion from various sources
2. **Formatted Zone**: Data standardization and cleaning
3. **Trusted Zone**: Quality assurance and validation
4. **Exploitation**: Data analysis and view creation
5. **Analysis**: Advanced analytics and insights generation

## Authors

- Alberto Jerez
- Jordi Granja
- Marta Carrión