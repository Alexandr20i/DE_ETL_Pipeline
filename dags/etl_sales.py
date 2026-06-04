from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from utils import get_mysql_conn, get_pg_conn, write_log

# Load to MRR
def extract_to_mrr():
    """
    1. Read HWM from MRR — the last time data was taken
    2. Go to OLTP — take only new records (delta)
    3. Write to mrr_dim_products
    4. Update HWM in MRR
    """

    started_at = datetime.now()
    dag_id = 'etl_sales'
    task_id = 'extract_to_mrr'

    try:
        mrr_conn = get_pg_conn('mrr')
        mrr_cur = mrr_conn.cursor()

        # Reading HWM — from which point to take data
        mrr_cur.execute("""
                       SELECT last_updated_at 
                       FROM high_water_mark 
                       WHERE table_name = 'sales'
                       """)
        hwm = mrr_cur.fetchone()[0]
        print(f"[MRR] HWM for sales: {hwm}")

        # Reading data from OLTP that has changed after HWM
        my_conn = get_mysql_conn()
        my_cur = my_conn.cursor()
    
        my_cur.execute("""                       
                          SELECT customer_id, product_id, qty, updated_at 
                          FROM sales
                          WHERE updated_at > %s
                          """, (hwm,))
        rows = my_cur.fetchall()
        print(f"[MRR] Extracted {len(rows)} rows from OLTP")

        # Writing to mrr
        if rows:
            mrr_cur.executemany("""
                INSERT INTO mrr_fact_sales (customer_id, product_id, qty, _loaded_at)
                VALUES (%s, %s, %s, NOW())
            """, [(row[0], row[1], row[2]) for row in rows])

            # Updating HWM — taking the maximum updated_at from what we loaded
            max_updated_at = max(row[3] for row in rows) 
            mrr_cur.execute("""
                UPDATE high_water_mark 
                SET last_updated_at = %s
                WHERE table_name = 'sales'
            """, (max_updated_at,))

        mrr_conn.commit()

        # Logging success
        write_log('mrr', dag_id, task_id, 'success', 
                  f"Records loaded: {len(rows)}", started_at)  
        
    except Exception as e:
        # Logging the error
        write_log('mrr', dag_id, task_id, 'error', str(e), started_at)
        raise

    finally:
        # Closing connections
        mrr_cur.close()
        mrr_conn.close()
        my_cur.close()
        my_conn.close()

# MRR -> STG
def load_to_stg():
    """
    Take from mrr_fact_sales everything that is not yet in stg    
    """

    started_at = datetime.now()
    dag_id = 'etl_sales'
    task_id = 'load_to_stg'

    try:
        # Reading HWM
        mrr_conn = get_pg_conn('mrr')
        mrr_cur = mrr_conn.cursor()

        stg_conn = get_pg_conn('stg')
        stg_cur = stg_conn.cursor()

        # Reading the last _loaded_at from stg — from which point to take data from mrr        
        stg_cur.execute("""
            SELECT MAX(_loaded_at) 
            FROM stg_fact_sales
        """)
        last_loaded = stg_cur.fetchone()[0]
        print(f"[STG] Last load: {last_loaded}")

        # Taking only new facts from mrr — delta by _loaded_at
        mrr_cur.execute("""
            SELECT customer_id, product_id, qty
            FROM mrr_fact_sales
            WHERE _loaded_at > %s
        """, (last_loaded or '1900-01-01',))
        rows = mrr_cur.fetchall()
        print(f"[STG] New records found: {len(rows)}")

        # INSERT — facts are never updated
        if rows:
            stg_cur.executemany("""
                INSERT INTO stg_fact_sales (customer_id, product_id, qty, _loaded_at)
                VALUES (%s, %s, %s, NOW())
            """, rows)

        stg_conn.commit()

        # Logging success
        write_log('stg', dag_id, task_id, 'success', 
                  f"Records loaded: {len(rows)}", started_at)
    
    except Exception as e:
        # Logging the error
        write_log('stg', dag_id, task_id, 'error', str(e), started_at)
        raise

    finally:
        mrr_cur.close()
        mrr_conn.close()
        stg_cur.close()
        stg_conn.close()

# STG -> DWH
def load_to_dwh():
    """
    Facts are not updated — only new records via delta by _loaded_at
    After loading, update HWM in dwh
    """

    started_at = datetime.now()
    dag_id = 'etl_sales'
    task_id = 'load_to_dwh'

    try:
        # Reading HWM
        stg_conn = get_pg_conn('stg')
        stg_cur = stg_conn.cursor()

        dwh_conn = get_pg_conn('dwh')
        dwh_cur = dwh_conn.cursor()

        # Reading the last _loaded_at from dwh — from which point to take data from stg
        dwh_cur.execute("""
            SELECT MAX(_loaded_at) 
            FROM dwh_fact_sales
        """)
        last_loaded = dwh_cur.fetchone()[0]
        print(f"[DWH] Last load: {last_loaded}")

        # Taking only new facts from stg — delta by _loaded_at
        stg_cur.execute("""
            SELECT customer_id, product_id, qty
            FROM stg_fact_sales
            WHERE _loaded_at > %s
        """, (last_loaded or '1900-01-01',))
        rows = stg_cur.fetchall()
        print(f"[DWH] New records found: {len(rows)}")

        # In DWH we simply add new facts, no need to update them
        if rows:
            dwh_cur.executemany("""
                INSERT INTO dwh_fact_sales (customer_id, product_id, qty, _loaded_at)
                VALUES (%s, %s, %s, NOW())
            """, rows)
        
        # Updating HWM — taking the maximum _loaded_at from what is stored in dwh
        dwh_cur.execute("""
            UPDATE high_water_mark 
            SET last_updated_at = (
                SELECT MAX(_loaded_at)
                FROM dwh_fact_sales
            )
            WHERE table_name = 'sales'
        """)
        dwh_conn.commit()

        write_log('dwh', dag_id, task_id, 'success', 
            f"Records loaded: {len(rows)}", started_at)    
        
    except Exception as e:
        # Logging the error
        write_log('dwh', dag_id, task_id, 'error', str(e), started_at)
        raise

    finally:
        # Closing connections
        stg_cur.close()
        stg_conn.close()
        dwh_cur.close()
        dwh_conn.close()


# Refresh sales by country mart
def refresh_dwh_mart_sales_by_country():
    """
    Rebuilding the dwh_mart_sales_by_country mart
    TRUNCATE + INSERT — full rebuild, products and sales facts
    """

    started_at = datetime.now()
    dag_id = 'etl_sales'
    task_id = 'refresh_dwh_mart_sales_by_country'

    try:
        dwh_conn = get_pg_conn('dwh')
        dwh_cur = dwh_conn.cursor()

        # Clearing the mart and rebuilding
        dwh_cur.execute("TRUNCATE TABLE dwh_mart_sales_by_country")

        # Inserting new data
        dwh_cur.execute("""
            INSERT INTO dwh_mart_sales_by_country 
                (country, total_qty, total_transactions, sales_month, _loaded_at)
            SELECT 
                c.country,
                SUM(s.qty)  AS total_qty,
                COUNT(*)    AS total_transactions,
                DATE_TRUNC('month', s._loaded_at) AS sales_month,
                NOW()       AS _loaded_at
            FROM 
                dwh_fact_sales s
            JOIN dwh_dim_customers c 
                ON s.customer_id = c.id 
                AND c.is_current = true
            GROUP BY 
                c.country, 
                DATE_TRUNC('month', s._loaded_at)
        """)

        dwh_conn.commit()

        # Logging success
        write_log('dwh', dag_id, task_id, 'success', 
                  "Mart dwh_mart_sales_by_country updated", started_at)
    
    except Exception as e:
        # Logging the error
        write_log('dwh', dag_id, task_id, 'error', str(e), started_at)
        raise

    finally:
        # Closing connections
        dwh_cur.close()
        dwh_conn.close()


# Refresh top products mart
def refresh_dwh_mart_top_products():
    """
    Rebuilding the dwh_mart_top_products mart
    TRUNCATE + INSERT — full rebuild, products and sales facts
    """

    started_at = datetime.now()
    dag_id = 'etl_sales'
    task_id = 'refresh_dwh_mart_top_products'

    try:
        dwh_conn = get_pg_conn('dwh')
        dwh_cur = dwh_conn.cursor()

        # Clearing the mart and rebuilding
        dwh_cur.execute("TRUNCATE TABLE dwh_mart_top_products")

        # Inserting new data
        dwh_cur.execute("""
            INSERT INTO dwh_mart_top_products 
                (product_name, group_name, total_qty, total_transactions, sales_month, _loaded_at)
            SELECT 
                p.name,
                p.group_name,
                SUM(s.qty)  AS total_qty,
                COUNT(*)    AS total_transactions,
                DATE_TRUNC('month', s._loaded_at) AS sales_month,
                NOW()       AS _loaded_at
            FROM 
                dwh_fact_sales s
            JOIN dwh_dim_products p 
                ON s.product_id = p.id 
            GROUP BY 
                p.id,
                p.name,
                p.group_name,
                DATE_TRUNC('month', s._loaded_at)
        """)

        dwh_conn.commit()

        # Logging success
        write_log('dwh', dag_id, task_id, 'success', 
                  "Mart dwh_mart_top_products updated", started_at)
    
    except Exception as e:
        # Logging the error
        write_log('dwh', dag_id, task_id, 'error', str(e), started_at)
        raise

    finally:
        # Closing connections
        dwh_cur.close()
        dwh_conn.close()

# dag
with DAG(
    'etl_sales',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['etl', 'sales']
) as dag:

    t1 = PythonOperator(
        task_id='extract_to_mrr',
        python_callable=extract_to_mrr
    )

    t2 = PythonOperator(
        task_id='load_to_stg',
        python_callable=load_to_stg
    )

    t3 = PythonOperator(
        task_id='load_to_dwh',
        python_callable=load_to_dwh
    )

    t4 = PythonOperator(
        task_id='refresh_dwh_mart_sales_by_country',
        python_callable=refresh_dwh_mart_sales_by_country
    )

    t5 = PythonOperator(
        task_id='refresh_dwh_mart_top_products',
        python_callable=refresh_dwh_mart_top_products
    )

    t1 >> t2 >> t3 >> [t4, t5]