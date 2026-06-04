\connect stg

CREATE TABLE IF NOT EXISTS stg_dim_customers (
    id          INT PRIMARY KEY,
    name        VARCHAR(100),
    country     VARCHAR(100),
    _loaded_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg_dim_products (
    id          INT PRIMARY KEY,
    name        VARCHAR(100),
    group_name  VARCHAR(100),
    _loaded_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg_fact_sales (
    customer_id INT,
    product_id  INT,
    qty         INT,
    _loaded_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_logs (
    id          SERIAL PRIMARY KEY,
    dag_id      VARCHAR(100),
    task_id     VARCHAR(100),
    status      VARCHAR(20),
    message     TEXT,
    started_at  TIMESTAMP,
    finished_at TIMESTAMP DEFAULT NOW()
);