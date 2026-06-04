\connect dwh 

-- Function: get_total_qty_by_country
-- Returns the total sales qty for the specified country
-- Uses current data (is_current = true)
CREATE OR REPLACE FUNCTION get_total_qty_by_country(p_country VARCHAR)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_qty INT;
BEGIN
    SELECT COALESCE(SUM(s.qty), 0)
    INTO v_total_qty
    FROM dwh_mart_sales s
    JOIN dwh_dim_customers c 
        ON s.customer_id = c.id 
        AND c.is_current = true
    WHERE c.country = p_country;
    
    RETURN v_total_qty;
END;
$$;


-- PROCEDURE: refresh_marts
-- Recalculates all marts using cursor
-- Writes log to etl_logs on error
CREATE OR REPLACE PROCEDURE refresh_marts()
LANGUAGE plpgsql
AS $$
DECLARE
    -- cursor — iterates over the list of marts to be recalculated
    v_mart_cursor CURSOR FOR 
        SELECT mart_name, truncate_sql, insert_sql 
        FROM (
            VALUES (
                'dwh_mart_sales_by_country'
                , 'TRUNCATE TABLE dwh_mart_sales_by_country'
                , 'INSERT INTO dwh_mart_sales_by_country 
                    (country, total_qty, total_transactions, sales_month, _loaded_at)
                SELECT 
                    c.country 
                    , SUM(s.qty) AS total_qty
                    , COUNT(s.sale_id) AS total_transactions
                    , DATE_TRUNC(''month'', s._loaded_at) AS sales_month
                    , NOW() AS _loaded_at
                FROM dwh_fact_sales s
                JOIN dwh_dim_customers c 
                    ON s.customer_id = c.id AND c.is_current = true
                GROUP BY 
                    c.country
                    , DATE_TRUNC(''month'', s._loaded_at)'
            ),
            (
                'dwh_mart_top_products'
                , 'TRUNCATE TABLE dwh_mart_top_products'
                , 'INSERT INTO dwh_mart_top_products
                (product_name, group_name, total_qty, total_transactions, sales_month, _loaded_at)
                SELECT 
                    p.name
                    , p.group_name
                    , SUM(s.qty) AS total_qty
                    , COUNT(*) AS total_transactions
                    , DATE_TRUNC(''month'', s._loaded_at) AS sales_month
                    , NOW() AS _loaded_at
                FROM dwh_fact_sales s
                JOIN dwh_dim_products p
                    ON s.product_id = p.id
                GROUP BY
                    p.name
                    , p.group_name
                    , DATE_TRUNC(''month'', s._loaded_at)'
            )
        )   AS marts(mart_name, truncate_sql, insert_sql);

    v_mart_name VARCHAR;
    v_truncate_sql TEXT;
    v_insert_sql TEXT;
    v_started_at TIMESTAMP;

BEGIN
    -- Opening cursor — starting to iterate over the marts
    OPEN v_mart_cursor;
    
    LOOP
        -- Fetching the next mart from the cursor
        FETCH v_mart_cursor INTO v_mart_name, v_truncate_sql, v_insert_sql;

        -- If no more marts — exit the loop
        EXIT WHEN NOT FOUND;

        -- Remembering the start time of the mart recalculation
        v_started_at := NOW();

        -- Attempting to recalculate the mart
        BEGIN
            -- Deleting old data from the mart
            EXECUTE v_truncate_sql;

            -- Inserting new data into the mart
            EXECUTE v_insert_sql;

            -- If everything went successfully — write a log about successful mart update
            INSERT INTO etl_logs (dag_id, task_id, status, message, created_at, finished_at)
            VALUES ('refresh_marts', v_mart_name, 'success', 'Mart refreshed successfully', v_started_at, NOW());

        -- If error — write a log and continue to the next mart
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO etl_logs (dag_id, task_id, status, message, created_at, finished_at)
            VALUES ('refresh_marts', v_mart_name, 'error', SQLERRM, v_started_at, NOW());
        END;

    END LOOP;

    -- Closing cursor
    CLOSE v_mart_cursor;
END;
$$;
