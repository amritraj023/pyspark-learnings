# PySpark Learnings

A comprehensive learning repository for Apache Spark using PySpark. This repository contains practical examples, exercises, and projects to master distributed data processing with Apache Spark.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup & Installation](#setup--installation)
- [Features](#features)
- [Getting Started](#getting-started)
- [Modules](#modules)
- [Sample Datasets](#sample-datasets)
- [Running Examples](#running-examples)
- [Contributing](#contributing)

## Overview

This repository contains a collection of PySpark learning materials designed to help you master:
- **Spark Fundamentals**: Core concepts and operations
- **Data Ingestion**: Reading data from various sources
- **DataFrame Operations**: Transformations and actions
- **Window Functions**: Advanced aggregation techniques
- **Data Joins**: Multi-table operations and relationships
- **Schema Management**: Explicit schema definition and validation

## Project Structure

```
pyspark-learnings/
├── src/
│   └── spark-fundamentals/
│       ├── ingestion/              # Data reading examples
│       │   ├── read_users.py
│       │   ├── read_products.py
│       │   ├── read_orders.py
│       │   ├── read_reviews.py
│       │   ├── read_events.py
│       │   ├── read_order_items.py
│       │   └── combined_reader.py
│       ├── explicit_schema/        # Schema definition examples
│       │   └── read_users_schema.py
│       ├── dataframes-joins/       # DataFrame join operations
│       │   ├── user-orders.py
│       │   ├── product-orders.py
│       │   └── customer-purchase-history.py
│       ├── window-functions/       # Window function examples
│       │   ├── window-reviews.py
│       │   └── window-orders.py
│       ├── utils/                  # Utility functions
│       │   └── read_utility.py
│       ├── spark-test.py           # Basic Spark session test
│       └── kaggle.py               # Kaggle dataset examples
├── data/                           # Sample CSV datasets
│   ├── events.csv
│   ├── order_items.csv
│   ├── orders.csv
│   ├── products.csv
│   ├── reviews.csv
│   └── users.csv
├── venv/                           # Python virtual environment
└── README.md
```

## Prerequisites

- **Python**: 3.7 or higher
- **Apache Spark**: 2.4 or higher (automatically installed via PySpark)
- **PySpark**: Latest stable version
- **Java Runtime**: Spark requires Java to run

## Setup & Installation

### 1. Clone the Repository
```bash
git clone https://github.com/amritraj023/pyspark-learnings.git
cd pyspark-learnings
```

### 2. Create Virtual Environment
```bash
python -m venv venv
```

### 3. Activate Virtual Environment

**On Windows:**
```bash
venv\Scripts\activate
```

**On macOS/Linux:**
```bash
source venv/bin/activate
```

### 4. Install Dependencies
```bash
pip install pyspark
```

## Features

### 📚 Data Ingestion
Learn how to read data from different file formats:
- CSV files
- Parquet files
- JSON data
- Database connections
- Multiple file aggregation

### 🔄 DataFrame Operations
Master core DataFrame operations:
- Selecting and filtering columns
- Creating new columns
- Type casting and transformations
- Data validation

### 🪟 Window Functions
Advanced analytical operations:
- Ranking functions (ROW_NUMBER, RANK, DENSE_RANK)
- Aggregation functions over windows
- Partitioning and ordering clauses

### 🔗 DataFrame Joins
Work with multi-table operations:
- Inner, Left, Right, and Full Outer joins
- Complex join conditions
- Data relationship analysis

### 📋 Schema Management
Define and validate data structures:
- Explicit schema definition
- Type safety
- Schema validation

## Getting Started

### Basic Example: Run a Simple Spark Program

```bash
python src/spark-fundamentals/spark-test.py
```

This creates a simple Spark session and displays a range of numbers 0-9.

### Working with CSV Data

```bash
python src/spark-fundamentals/ingestion/read_users.py
```

This reads the `users.csv` dataset and displays sample records.

### Using Explicit Schema

```bash
python src/spark-fundamentals/explicit_schema/read_users_schema.py
```

This demonstrates reading data with an explicitly defined schema for type safety.

## Modules

### Ingestion Module
Located in `src/spark-fundamentals/ingestion/`

Examples for reading different datasets:
- `read_users.py` - Read user data
- `read_products.py` - Read product information
- `read_orders.py` - Read order transactions
- `read_reviews.py` - Read customer reviews
- `read_events.py` - Read event logs
- `read_order_items.py` - Read order line items
- `combined_reader.py` - Aggregate multiple data sources

### Window Functions Module
Located in `src/spark-fundamentals/window-functions/`

Advanced analytical techniques:
- `window-orders.py` - Order analysis with window functions
- `window-reviews.py` - Review metrics with ranking

### Joins Module
Located in `src/spark-fundamentals/dataframes-joins/`

Multi-table relationship operations:
- `user-orders.py` - Join users with their orders
- `product-orders.py` - Join products with orders
- `customer-purchase-history.py` - Analyze customer purchase patterns

### Schema Module
Located in `src/spark-fundamentals/explicit_schema/`

Strongly typed data operations:
- `read_users_schema.py` - Define and validate user schema

### Utils Module
Located in `src/spark-fundamentals/utils/`

Helper functions:
- `read_utility.py` - Common utility functions for data operations

## Sample Datasets

The repository includes sample CSV datasets in the `data/` directory:

| File | Description | Records |
|------|-------------|---------|
| `users.csv` | User profiles with demographics | N users |
| `products.csv` | Product catalog | N products |
| `orders.csv` | Order transactions | N orders |
| `reviews.csv` | Customer reviews | N reviews |
| `events.csv` | Event logs | N events |
| `order_items.csv` | Order line items | N items |

## Running Examples

### Run All Modules

Navigate to `src/spark-fundamentals/` and execute any Python script:

```bash
# Run data ingestion
python src/spark-fundamentals/ingestion/read_users.py

# Run window functions
python src/spark-fundamentals/window-functions/window-orders.py

# Run joins
python src/spark-fundamentals/dataframes-joins/user-orders.py
```

### Expected Output

Each script will display:
- DataFrame schema (column names and types)
- Sample rows from the dataset
- Transformation results
- Summary statistics

### Troubleshooting

**Issue**: Java not found
- **Solution**: Install Java JDK 8 or higher and set `JAVA_HOME` environment variable

**Issue**: Module not found errors
- **Solution**: Ensure you're running scripts from the repository root and the virtual environment is activated

**Issue**: Data files not found
- **Solution**: Verify that CSV files exist in the `data/` directory

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

### Guidelines

- Add examples that demonstrate new PySpark concepts
- Include comments explaining complex operations
- Update documentation and this README when adding new modules
- Test all examples before submitting


## Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Apache Spark Official Guide](https://spark.apache.org/docs/latest/)
- [Spark SQL API](https://spark.apache.org/docs/latest/sql-programming-guide.html)

## Contact & Support

For questions or issues, please open an issue on GitHub.

---

Happy Learning! 🚀
