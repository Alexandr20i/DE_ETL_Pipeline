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
    dag_id = 'etl_customers'
    task_id = 'extract_to_mrr'

    try:
        # Reading HWM
        pg_conn = get_pg_conn('mrr')
        pg_cur = pg_conn.cursor()
        
        # Reading HWM — from which point to take data
        pg_cur.execute("""
                       SELECT last_updated_at 
                       FROM high_water_mark 
                       WHERE table_name = 'customers'
                       """)
        hwm = pg_cur.fetchone()[0]
        print(f"[MRR] HWM for sales: {hwm}")

        # Going to OLTP, taking only the delta
        my_conn = get_mysql_conn()
        my_cur = my_conn.cursor()

        my_cur.execute(""" SELECT id, name, country, updated_at 
                       FROM customers 
                       WHERE updated_at > %s 
                       """, (hwm,))
        rows = my_cur.fetchall()
        print(f"[MRR] Extracted {len(rows)} rows from OLTP")

        # Writing to mrr
        if rows:
            pg_cur.executemany("""
                INSERT INTO mrr_dim_customers (id, name, country, _loaded_at)
                VALUES (%s, %s, %s, NOW())
            """, [(row[0], row[1], row[2]) for row in rows])

            # Updating HWM — taking the maximum updated_at from what we loaded
            max_updated_at = max(r[3] for r in rows)
            pg_cur.execute("""
                UPDATE high_water_mark 
                SET last_updated_at = %s 
                WHERE table_name = 'customers'
            """, (max_updated_at,))

        pg_conn.commit()

        # Logging success
        write_log('mrr', dag_id, task_id, 'success', 
                  f"Records loaded: {len(rows)}", started_at)  
        
    except Exception as e:
        # Logging the error
        write_log('mrr', dag_id, task_id, 'error', str(e), started_at)  
        raise 
    
    finally:
        # Closing connections
        my_cur.close()
        my_conn.close()
        pg_cur.close()
        pg_conn.close()

# MRR -> STG
def load_to_stg():
    """
    Take from mrr_dim_customers everything that is not yet in stg
    - if the customer is new → INSERT
    - if the customer already exists → UPDATE (name, country may have changed)
    STG always stores the current version, without history
    """

    started_at = datetime.now()
    dag_id = 'etl_customers'
    task_id = 'load_to_stg'

    try:
        # Reading HWM
        mrr_conn = get_pg_conn('mrr')
        mrr_cur = mrr_conn.cursor()

        stg_conn = get_pg_conn('stg')
        stg_cur = stg_conn.cursor()

        mrr_cur.execute("""
            SELECT id, name, country 
            FROM mrr_dim_customers
        """)
        rows = mrr_cur.fetchall()
        print(f"[STG] Processing {len(rows)} rows from MRR")


        # If id already exists — update name and country
        if rows:
            stg_cur.executemany("""
                INSERT INTO stg_dim_customers (id, name, country, _loaded_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE 
                SET name        = EXCLUDED.name, 
                    country     = EXCLUDED.country,
                    _loaded_at  = NOW()
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
        mrr_cur.close()
        mrr_conn.close()
        stg_cur.close()
        stg_conn.close()

# STG -> DWH 
def load_to_dwh():
    """
    SCD Type 2 logic:
    For each customer from STG:
    1. Find the current record in DWH (is_current = true)
    2. If not found → new customer, just INSERT
    3. If found and country has NOT changed → do nothing
    4. If found and country HAS changed:
       - old record: is_current = false, valid_to = today
       - new record: is_current = true,  valid_from = today
    """

    started_at = datetime.now()
    dag_id = 'etl_customers'
    task_id = 'load_to_dwh'

    try:
        # Reading HWM
        stg_conn = get_pg_conn('stg')
        stg_cur = stg_conn.cursor()

        dwh_conn = get_pg_conn('dwh')
        dwh_cur = dwh_conn.cursor()

        # Taking everything from stg
        stg_cur.execute("""
            SELECT id, name, country 
            FROM stg_dim_customers
        """)
        rows = stg_cur.fetchall()
        print(f"[DWH] Processing records: {len(rows)}")

        inserted = 0
        updated = 0
        skipped = 0

        for row in rows:
            src_id, src_name, src_country = row

            # Looking for the current record in DWH
            dwh_cur.execute("""
                SELECT surrogate_id, country 
                FROM dwh_dim_customers 
                WHERE id = %s AND is_current = true
            """, (src_id,))
            existing = dwh_cur.fetchone()

            if existing is None:
                # New customer — insert
                dwh_cur.execute("""
                    INSERT INTO dwh_dim_customers 
                            (id, name, country, valid_from, valid_to, is_current, _loaded_at)
                    VALUES (%s, %s, %s, CURRENT_DATE, '9999-12-31', true, NOW())
                """, (src_id, src_name, src_country))
                inserted += 1

            elif existing[1] != src_country:
                # Country has changed -> SCD Type 2
                surrogate_id = existing[0]

                # Closing the old record
                dwh_cur.execute("""
                    UPDATE dwh_dim_customers 
                    SET is_current  = false, 
                        valid_to    = CURRENT_DATE
                    WHERE surrogate_id = %s
                """, (surrogate_id,))

                # Inserting the new version
                dwh_cur.execute("""
                    INSERT INTO dwh_dim_customers 
                            (id, name, country, valid_from, valid_to, is_current, _loaded_at)
                    VALUES (%s, %s, %s, CURRENT_DATE, '9999-12-31', true, NOW())
                """, (src_id, src_name, src_country))
                updated += 1

            else:
                # Country hasn't changed — skipping
                skipped += 1

        dwh_conn.commit()

        # Updating HWM in dwh — taking the maximum _loaded_at from dwh_dim_customers
        dwh_cur.execute("""
            UPDATE high_water_mark
            SET last_updated_at = (
                        SELECT MAX(_loaded_at) FROM dwh_dim_customers
            )
            WHERE table_name = 'customers'
        """)
        dwh_conn.commit()

        msg = f'Inserted: {inserted}, Updated(SCD2): {updated}, Skipped: {skipped}'
        print(f"[DWH] {msg}")

        # Logging success
        write_log('dwh', dag_id, task_id, 'success', msg, started_at)

        
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
    dag_id='etl_customers',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'customers']
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

