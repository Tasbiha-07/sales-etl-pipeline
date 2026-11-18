"""
Sales ETL pipeline: CSV -> clean/validate -> star-schema warehouse (MySQL or SQLite) -> analytical SQL.

Usage
  python etl_pipeline.py --csv your_sales.csv                         # SQLite (no setup needed)
  python etl_pipeline.py --csv your_sales.csv --db "mysql+pymysql://user:password@localhost:3306/sales_dw"
  python etl_pipeline.py --demo                                       # synthetic data, only to test the pipeline

Before running on your own file, edit COLUMN_MAP below so the values match YOUR csv column names.
"""
import argparse
import logging
import os
import random
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

# ---- EDIT THIS: left = name used by the pipeline, right = column name in YOUR csv ----
COLUMN_MAP = {
    "order_id": "order_id",
    "order_date": "order_date",
    "customer": "customer",
    "product": "product",
    "category": "category",      # set to None if your file has no category column
    "region": "region",
    "quantity": "quantity",
    "unit_price": "unit_price",  # if you only have a total, set this to None and set revenue below
    "revenue": None,             # optional: column holding the line total
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("etl")
HERE = os.path.dirname(os.path.abspath(__file__))


# ------------------------------------------------------------------ demo data
def make_demo_csv(path, n=10000, seed=42):
    """Synthetic data (with deliberate duplicates and bad rows) just to test the pipeline."""
    random.seed(seed)
    cats = {"Electronics": ["Phone", "Laptop", "Headphones", "Tablet", "Camera", "Speaker", "Monitor", "Router"],
            "Home": ["Mixer", "Lamp", "Kettle", "Vacuum", "Fan", "Cooker", "Heater", "Iron"],
            "Fashion": ["Shirt", "Jeans", "Jacket", "Shoes", "Watch", "Bag", "Cap", "Belt"],
            "Sports": ["Racket", "Football", "Yoga Mat", "Dumbbell", "Bike Helmet", "Gloves", "Rope", "Bottle"],
            "Books": ["Novel", "Textbook", "Comic", "Journal", "Atlas", "Cookbook", "Guide", "Biography"]}
    products = [(p, c) for c, ps in cats.items() for p in ps]
    regions = ["North", "South", "East", "West"]
    rows = []
    for i in range(n):
        p, c = random.choice(products)
        d = date(2025, 1, 1) + timedelta(days=random.randint(0, 364))
        rows.append([f"ORD{i+1:06d}", d.isoformat(), f"Customer {random.randint(1, 300):03d}", p, c,
                     random.choice(regions), random.randint(1, 8), round(random.uniform(5, 500), 2)])
    df = pd.DataFrame(rows, columns=["order_id", "order_date", "customer", "product", "category",
                                     "region", "quantity", "unit_price"])
    dup = df.sample(150, random_state=1)                       # duplicate orders
    bad = df.sample(100, random_state=2).copy()
    bad.loc[bad.index[:50], "quantity"] = -1                   # invalid quantity
    bad.loc[bad.index[50:], "customer"] = None                 # missing customer
    bad["order_id"] = [f"BAD{i:04d}" for i in range(len(bad))]
    pd.concat([df, dup, bad]).sample(frac=1, random_state=3).to_csv(path, index=False)
    log.info("Demo CSV written to %s (synthetic data - do NOT present it as real data)", path)


# ------------------------------------------------------------------ extract
def extract(csv_path):
    df = pd.read_csv(csv_path)
    log.info("Extracted %d rows, %d columns from %s", len(df), df.shape[1], csv_path)
    return df


# ------------------------------------------------------------------ transform
def transform(raw):
    report = {"rows_extracted": len(raw)}
    m = {k: v for k, v in COLUMN_MAP.items() if v}
    missing = [v for v in m.values() if v not in raw.columns]
    if missing:
        raise SystemExit(f"These columns from COLUMN_MAP are not in your CSV: {missing}. Edit COLUMN_MAP.")
    df = raw.rename(columns={v: k for k, v in m.items()})[list(m.keys())].copy()

    if "category" not in df:
        df["category"] = "Uncategorized"
    if "unit_price" not in df and "revenue" in df:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["unit_price"] = pd.to_numeric(df["revenue"], errors="coerce") / df["quantity"]

    before = len(df)
    df = df.drop_duplicates(subset="order_id", keep="first")
    report["duplicates_removed"] = before - len(df)

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    for col in ["customer", "product", "region", "category"]:
        df[col] = df[col].astype("string").str.strip()

    before = len(df)
    valid = (df[["order_id", "order_date", "customer", "product", "region", "quantity", "unit_price"]]
             .notna().all(axis=1)) & (df["quantity"] > 0) & (df["unit_price"] > 0)
    df = df[valid].copy()
    report["invalid_rows_removed"] = before - len(df)

    df["quantity"] = df["quantity"].astype(int)
    df["revenue"] = (df["quantity"] * df["unit_price"]).round(2)
    df["order_month"] = df["order_date"].dt.strftime("%Y-%m")
    df["order_date"] = df["order_date"].dt.strftime("%Y-%m-%d")
    df["category"] = df["category"].fillna("Uncategorized")
    report["rows_clean"] = len(df)
    log.info("Transform done: %s", report)
    return df, report


# ------------------------------------------------------------------ load
def build_dim(df, cols, id_col):
    dim = df[cols].drop_duplicates().sort_values(cols).reset_index(drop=True)
    dim.insert(0, id_col, dim.index + 1)
    return dim


def run_sql_script(conn, path):
    with open(path, encoding="utf-8") as f:
        for stmt in f.read().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def load(df, engine, report):
    dim_c = build_dim(df, ["customer"], "customer_id").rename(columns={"customer": "customer_name"})
    dim_p = build_dim(df, ["product", "category"], "product_id").rename(columns={"product": "product_name"})
    dim_r = build_dim(df, ["region"], "region_id").rename(columns={"region": "region_name"})

    fact = (df.merge(dim_c, left_on="customer", right_on="customer_name")
              .merge(dim_p, left_on=["product", "category"], right_on=["product_name", "category"])
              .merge(dim_r, left_on="region", right_on="region_name"))
    fact = fact[["order_id", "order_date", "order_month", "customer_id", "product_id", "region_id",
                 "quantity", "unit_price", "revenue"]]

    with engine.begin() as conn:
        run_sql_script(conn, os.path.join(HERE, "schema.sql"))
        dim_c.to_sql("dim_customer", conn, if_exists="append", index=False)
        dim_p.to_sql("dim_product", conn, if_exists="append", index=False)
        dim_r.to_sql("dim_region", conn, if_exists="append", index=False)
        fact.to_sql("fact_sales", conn, if_exists="append", index=False, chunksize=2000)

    report.update(rows_loaded=len(fact), dim_customers=len(dim_c), dim_products=len(dim_p), dim_regions=len(dim_r))
    log.info("Loaded %d fact rows and %d/%d/%d dimension rows", len(fact), len(dim_c), len(dim_p), len(dim_r))


# ------------------------------------------------------------------ analyse
def run_queries(engine, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(HERE, "queries.sql"), encoding="utf-8") as f:
        blocks = [b for b in f.read().split("-- name:") if b.strip()]
    results = {}
    with engine.connect() as conn:
        for b in blocks:
            name, _, sql = b.partition("\n")
            name = name.strip()
            res = pd.read_sql(text(sql.strip().rstrip(";")), conn)
            res.to_csv(os.path.join(out_dir, f"{name}.csv"), index=False)
            results[name] = res
            log.info("Query '%s' returned %d rows", name, len(res))
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="path to your sales CSV")
    ap.add_argument("--db", default=f"sqlite:///{os.path.join(HERE, 'sales_dw.db')}", help="SQLAlchemy DB URL")
    ap.add_argument("--demo", action="store_true", help="generate synthetic data to test the pipeline")
    a = ap.parse_args()

    if a.demo:
        a.csv = os.path.join(HERE, "demo_sales.csv")
        make_demo_csv(a.csv)
    if not a.csv:
        raise SystemExit("Give --csv your_file.csv (or --demo)")

    engine = create_engine(a.db)
    clean, report = transform(extract(a.csv))
    load(clean, engine, report)
    results = run_queries(engine, os.path.join(HERE, "outputs"))
    report["orphan_rows_in_fact_table"] = int(results["orphan_check_customers"].iloc[0, 0])

    lines = ["PIPELINE REPORT"] + [f"{k}: {v}" for k, v in report.items()]
    with open(os.path.join(HERE, "pipeline_report.txt"), "w") as f:
        f.write("\n".join(lines))
    print("\n" + "\n".join(lines))
    print("\nMonthly revenue trend (first 5 rows):\n", results["monthly_revenue_trend"].head())


if __name__ == "__main__":
    main()
