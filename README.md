# data-master-healthlake

End-to-end healthcare data engineering platform built with Azure Data Factory, Azure Databricks and Delta Lake following Medallion Architecture principles.

## Data generator

The synthetic generator produces healthcare source snapshots partitioned by `odate`.
Each new partition loads the latest previous snapshot, applies controlled churn, appends new records and injects realistic raw-data issues such as optional fields with null values and occasional duplicate rows.

```bash
cd data-generator
python -m venv ../venv
../venv/Scripts/pip install -r requirements.txt
../venv/Scripts/python main.py --odate 2026-05-20 --seed 42
../venv/Scripts/python main.py --odate 2026-05-21 --seed 43
```

Generated files are written to:

```text
data-generator/output/raw/odate=YYYY-MM-DD/
```

To replace an existing partition intentionally:

```bash
../venv/Scripts/python main.py --odate 2026-05-21 --seed 43 --overwrite
```

To upload a generated partition to S3 for the ADF batch ingestion flow:

```bash
../venv/Scripts/python ingestion-s3/upload_to_s3.py --odate 2026-05-21
```

Run the generator test suite with:

```bash
../venv/Scripts/python -m unittest discover -s tests
```
