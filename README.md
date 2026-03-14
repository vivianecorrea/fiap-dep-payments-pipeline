# Sales Report Pipeline (PySpark)
## FIAP Data Engineering Programming Final Project


This project implements a PySpark data pipeline that generates a sales report for orders whose payments were refused and classified as legitimate by fraud analysis.

The final report includes only orders created in **2025** and is sorted by **state, payment method, and order date**.


# Project Structure
```
fiap-dep-payments-pipeline
│
├── src
│   └── sales.py
│
├── scripts
│   └── setup_datasets.sh
│
├── tests
│   ├── conftest.py
│   ├── test_business_requirements.py
│   ├── test_transformations.py
│   └── test_format_output.py
│
├── datasets
├── requirements.txt
└── README.md
```

# Environment Setup

Clone the repository:

git clone https://github.com/vivianecorrea/fiap-dep-payments-pipeline.git
cd fiap-dep-payments-pipeline

Create a virtual environment:

python3 -m venv .venv

Activate the environment:

source .venv/bin/activate

Install dependencies:

pip install -r requirements.txt


# Download Source Datasets

Before running the pipeline, download the source datasets:

sh ./scripts/setup_datasets.sh


# Running the Pipeline

Run the pipeline with:

python src/sales.py

The report will be saved in:

datasets/output/sales_report_2025/


# Running Tests

Run all tests:

pytest

Run tests with verbose output:

pytest -v
