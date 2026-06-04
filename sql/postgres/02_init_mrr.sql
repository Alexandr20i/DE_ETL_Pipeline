\connect mrr

CREATE TABLE IF NOT EXISTS mrr_dim_customers (
    id          INT,
    name        VARCHAR(100),
    country     VARCHAR(100),
    _loaded_at  TIMESTAMP DEFAULT NOW()
);
    
CREATE TABLE IF NOT EXISTS mrr_dim_products (
    id          INT,
    name        VARCHAR(100),
    group_name  VARCHAR(100),
    _loaded_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mrr_fact_sales (
    customer_id INT,
    product_id  INT,
    qty         INT,
    _loaded_at  TIMESTAMP DEFAULT NOW()
);

-- High Water Mark
CREATE TABLE IF NOT EXISTS high_water_mark (
    table_name      VARCHAR(100) PRIMARY KEY,
    last_updated_at TIMESTAMP NOT NULL DEFAULT '1900-01-01'
);

INSERT INTO high_water_mark (table_name, last_updated_at) VALUES
    ('customers', '1900-01-01'),
    ('products',  '1900-01-01'),
    ('sales',     '1900-01-01')
ON CONFLICT (table_name) DO NOTHING;

-- ETL logs
CREATE TABLE IF NOT EXISTS etl_logs (
    id          SERIAL PRIMARY KEY,
    dag_id      VARCHAR(100),
    task_id     VARCHAR(100),
    status      VARCHAR(20),  -- success / error
    message     TEXT,
    started_at  TIMESTAMP,
    finished_at TIMESTAMP DEFAULT NOW()
);