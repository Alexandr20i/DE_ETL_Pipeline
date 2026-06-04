\connect dwh

CREATE TABLE IF NOT EXISTS dwh_dim_customers (
    surrogate_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- surrogate key, unique for each record version
    id            INT NOT NULL,         -- original id from the source
    name          VARCHAR(100),
    country       VARCHAR(100),
    valid_from    DATE    NOT NULL DEFAULT CURRENT_DATE,
    valid_to      DATE    NOT NULL DEFAULT '9999-12-31',
    is_current    BOOLEAN NOT NULL DEFAULT true,
    _loaded_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh_dim_products (
    id          INT PRIMARY KEY,
    name        VARCHAR(100),
    group_name  VARCHAR(100),
    _loaded_at  TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS dwh_fact_sales (
    customer_id INT,
    product_id  INT,
    qty         INT,
    _loaded_at  TIMESTAMP DEFAULT NOW()
);

-- Mart 1: sales by country
CREATE TABLE IF NOT EXISTS dwh_mart_sales_by_country (
    country             VARCHAR(100),
    total_qty           INT,
    total_transactions  INT,
    sales_month         DATE,
    _loaded_at          TIMESTAMP DEFAULT NOW()
);

-- Mart 2: top products
CREATE TABLE IF NOT EXISTS dwh_mart_top_products (
    product_name        VARCHAR(100),
    group_name          VARCHAR(100),
    total_qty           INT,
    total_transactions  INT,
    sales_month         DATE,
    _loaded_at          TIMESTAMP DEFAULT NOW()
);

-- High Water Mark is updated here after loading into dwh
CREATE TABLE IF NOT EXISTS high_water_mark (
    table_name      VARCHAR(100) PRIMARY KEY,
    last_updated_at TIMESTAMP NOT NULL DEFAULT '1900-01-01'
);

INSERT INTO high_water_mark (table_name, last_updated_at) VALUES
    ('customers', '1900-01-01'),
    ('products',  '1900-01-01'),
    ('sales',     '1900-01-01')
ON CONFLICT (table_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS etl_logs (
    id          SERIAL PRIMARY KEY,
    dag_id      VARCHAR(100),
    task_id     VARCHAR(100),
    status      VARCHAR(20),
    message     TEXT,
    started_at  TIMESTAMP,
    finished_at TIMESTAMP DEFAULT NOW()
);