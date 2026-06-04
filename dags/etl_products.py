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
    dag_id = 'etl_products'
    task_id = 'extract_to_mrr'

    try:
        # Reading HWM
        mrr_conn = get_pg_conn('mrr')
        mrr_cur = mrr_conn.cursor()

        # Reading HWM — from which point to take data
        mrr_cur.execute("""
                       SELECT last_updated_at 
                       FROM high_water_mark 
                       WHERE table_name = 'products'
                       """)
        hwm = mrr_cur.fetchone()[0]
        print(f"[MRR] HWM для products: {hwm}")

        # Reading data from OLTP that has changed after HWM
        my_conn = get_mysql_conn()
        my_cur = my_conn.cursor()
    
        my_cur.execute("""                       
                       SELECT id, name, group_name, updated_at 
                       FROM products
                       WHERE updated_at > %s
                       """, (hwm,))
        rows = my_cur.fetchall()
        print(f"[MRR] Extracted {len(rows)} rows from OLTP")

        # Writing to mrr
        if rows:
            mrr_cur.executemany("""
                INSERT INTO mrr_dim_products (id, name, group_name, _loaded_at) 
                VALUES (%s, %s, %s, NOW())
            """, [(row[0], row[1], row[2]) for row in rows])

            # Updating HWM — taking the maximum updated_at from what we loaded
            max_updated_at = max(row[3] for row in rows)
            mrr_cur.execute("""
                UPDATE high_water_mark 
                SET last_updated_at = %s
                WHERE table_name = 'products'
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
    STG always stores the current version of the product without history (SCD Type 1)
    - if the product is new -> INSERT
    - if the product already exists -> UPDATE (name, group_name may have changed)
    """

    started_at = datetime.now()
    dag_id = 'etl_products'
    task_id = 'load_to_stg'

    try:
        # Reading HWM
        mrr_conn = get_pg_conn('mrr')
        mrr_cur = mrr_conn.cursor() 

        stg_conn = get_pg_conn('stg')
        stg_cur = stg_conn.cursor()

        mrr_cur.execute("""
            SELECT id, name, group_name
            FROM mrr_dim_products
        """)
        rows = mrr_cur.fetchall()
        print(f"[STG] Processing {len(rows)} rows from MRR")

        # If id already exists — update name and group_name
        if rows:
            stg_cur.executemany("""
                INSERT INTO stg_dim_products (id, name, group_name, _loaded_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE 
                SET 
                    name = EXCLUDED.name,
                    group_name = EXCLUDED.group_name,
                    _loaded_at = NOW()
            """, rows)
        
        stg_conn.commit()

        # Logging success
        write_log('stg', dag_id, task_id, 'success', 
                  f"Records loaded/updated: {len(rows)}", started_at)
    
    except Exception as e:
        # Logging the error
        write_log('stg', dag_id, task_id, 'error', str(e), started_at)
        raise

    finally:
        # Closing connections
        mrr_cur.close()
        mrr_conn.close()
        stg_cur.close()
        stg_conn.close()


# STG -> DWH 
def load_to_dwh():
    """
    SCD Type 1 — history is not stored, just overwrite the current data
    - if the product is new -> INSERT
    - if the product already exists -> UPDATE (name, group_name may have changed)
    After loading, update the HWM in dwh
    """

    started_at = datetime.now()
    dag_id = 'etl_products'
    task_id = 'load_to_dwh'

    try:
        # Reading HWM
        stg_conn = get_pg_conn('stg')
        stg_cur = stg_conn.cursor() 

        dwh_conn = get_pg_conn('dwh')
        dwh_cur = dwh_conn.cursor()

        # Taking everything from stg
        stg_cur.execute("""
            SELECT id, name, group_name
            FROM stg_dim_products
        """)
        rows = stg_cur.fetchall()
        print(f"[DWH] Processing records {len(rows)} rows from STG")

        if rows:
            dwh_cur.executemany("""
                INSERT INTO dwh_dim_products (id, name, group_name, _loaded_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE 
                SET 
                    name = EXCLUDED.name,
                    group_name = EXCLUDED.group_name,
                    _loaded_at = NOW()
            """, rows)
        
        # Updating HWM — taking the maximum _loaded_at from what is stored in dwh
        dwh_cur.execute("""
            UPDATE high_water_mark
            SET last_updated_at = (
                SELECT MAX(_loaded_at) FROM dwh_dim_products
            )
            WHERE table_name = 'products'
        """)
        dwh_conn.commit()
        
        # Logging success
        write_log('dwh', dag_id, task_id, 'success', 
            f"Records loaded/updated: {len(rows)}", started_at)

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

# dag
with DAG(
    'etl_products',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags = ['etl', 'products']
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

    t1 >> t2 >> t3
