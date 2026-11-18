# Sales ETL Pipeline (Python + SQL data warehouse)

An end-to-end ETL pipeline: extracts raw sales CSV data, cleans and validates it with Pandas,
loads it into a star-schema data warehouse (MySQL or SQLite), and runs analytical SQL
(joins, GROUP BY, window functions) on top of it.

## Architecture
CSV -> Extract (Pandas) -> Transform (dedupe, type checks, validation, derived columns)
    -> Load (star schema: fact_sales + dim_customer / dim_product / dim_region, with keys and indexes)
    -> Analyse (SQL queries -> CSV outputs + pipeline_report.txt)

## Data-quality checks
- Removes duplicate order IDs
- Rejects rows with missing keys, invalid dates, or non-positive quantity/price
- Computes revenue and order month
- Orphan check on foreign keys after load

## Run it
    pip install -r requirements.txt
    python etl_pipeline.py --csv your_sales.csv                     # SQLite, no setup
    python etl_pipeline.py --csv your_sales.csv --db "mysql+pymysql://user:password@localhost:3306/sales_dw"

(Create the database first in MySQL: `CREATE DATABASE sales_dw;`)
Edit `COLUMN_MAP` at the top of `etl_pipeline.py` to match your CSV's column names.

## Analytical queries (queries.sql)
Monthly revenue with month-over-month change (LAG), top 3 products per region (RANK),
category revenue share (window SUM), repeat customers (HAVING), orphan-row check (LEFT JOIN).

## Results
(Fill these in from your own pipeline_report.txt after running on your real data.)
