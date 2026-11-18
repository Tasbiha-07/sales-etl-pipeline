-- Star schema for the sales data warehouse (works on MySQL 8 and SQLite)
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_region;

CREATE TABLE dim_customer (
    customer_id   INTEGER PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL
);

CREATE TABLE dim_product (
    product_id   INTEGER PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category     VARCHAR(100) NOT NULL
);

CREATE TABLE dim_region (
    region_id   INTEGER PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL
);

CREATE TABLE fact_sales (
    order_id    VARCHAR(50) PRIMARY KEY,
    order_date  DATE NOT NULL,
    order_month CHAR(7) NOT NULL,
    customer_id INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    region_id   INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    unit_price  DECIMAL(12,2) NOT NULL,
    revenue     DECIMAL(14,2) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
    FOREIGN KEY (product_id)  REFERENCES dim_product (product_id),
    FOREIGN KEY (region_id)   REFERENCES dim_region (region_id)
);

CREATE INDEX idx_fact_month   ON fact_sales (order_month);
CREATE INDEX idx_fact_product ON fact_sales (product_id);
CREATE INDEX idx_fact_region  ON fact_sales (region_id);
CREATE INDEX idx_fact_cust    ON fact_sales (customer_id);
