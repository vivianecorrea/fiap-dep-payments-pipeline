# Sales Report Pipeline (PySpark)
## FIAP Data Engineering Programming Final Project

This project implements a PySpark data pipeline that generates a sales report for orders whose payments were refused and classified as legitimate by fraud analysis.

The final report includes only orders created in **2025** and is sorted by **state, payment method, and order date**.

---

## Project Structure

```
fiap-dep-payments-pipeline/
│
├── main.py                         # Aggregation Root (entry point, DI wiring)
├── pyproject.toml                  # Project packaging
├── requirements.txt                # Python dependencies
├── MANIFEST.in                     # Package manifest
│
├── config/
│   ├── app_config.py               # AppConfig class (centralized configuration)
│   └── settings.yaml               # YAML configuration (paths, year, spark settings)
│
├── src/
│   ├── spark/
│   │   └── spark_session_manager.py  # SparkSessionManager class
│   ├── io/
│   │   └── data_io.py              # DataIO class (read/write abstraction)
│   ├── business/
│   │   ├── columns.py              # Column name constants
│   │   ├── schemas.py              # Explicit schema definitions
│   │   └── sales_report.py         # SalesReportBuilder (business logic)
│   └── pipeline/
│       └── pipeline.py             # PaymentsReportPipeline (orchestration)
│
├── tests/
│   ├── conftest.py
│   ├── test_business_requirements.py
│   ├── test_transformations.py
│   └── test_format_output.py
│
└── datasets/                       # Source and output data (not committed)
```

---

## Environment Setup

Clone the repository:

```bash
git clone https://github.com/vivianecorrea/fiap-dep-payments-pipeline.git
cd fiap-dep-payments-pipeline
```

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Download Source Datasets

Before running the pipeline, download the source datasets:

```bash
sh ./scripts/setup_datasets.sh
```

The datasets will be placed under `fiap-dep-payments-pipeline/datasets/`.

---

## Running the Pipeline

```bash
python main.py
```

The report will be saved in parquet format at the path configured in `config/settings.yaml` (default: `datasets/output/sales_report/`).

To change the report year or dataset paths, edit `config/settings.yaml`:

```yaml
pipeline:
  report_year: 2025
```

---

## Running Tests

```bash
pytest
```

With verbose output:

```bash
pytest -v
```
