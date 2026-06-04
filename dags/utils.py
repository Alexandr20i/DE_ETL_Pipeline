import pymysql
import psycopg2
from datetime import datetime

def get_mysql_conn():
    """
    Connection to MySQL (OLTP)    
    """
    return pymysql.connect(
        host='mysql_source',
        user='etl_user',
        password='etl_pass',
        database='operational_db'
    )

def get_pg_conn(database):
    """
    Connection to PostgreSQL — passing the database name: mrr, stg, dwh
    """
    return psycopg2.connect(
        host='postgres_dwh',
        user='dwh_user',
        password='dwh_pass',
        database=database
    )

def write_log(database, dag_id, task_id, status, message, started_at):
    """
    Writing log to etl_logs
    """
    conn = get_pg_conn(database)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO etl_logs 
                (dag_id, task_id, status, message, started_at, finished_at) 
                VALUES (%s, %s, %s, %s, %s, NOW())
    """, (dag_id, task_id, status, message, started_at))
    conn.commit()
    cur.close()
    conn.close()