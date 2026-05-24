from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

DB_URL = "postgresql://admin:admin123@postgres:5432/streamflow_db"  

default_args = {
    "owner": "streamflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def check_db_connection(**context):
    from sqlalchemy import create_engine, text
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1")).fetchone()
    logger.info("✅ Database connection verified.")
    engine.dispose()


def count_old_records(**context):
    from sqlalchemy import create_engine, text
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        count = conn.execute(text("""
            SELECT COUNT(*) FROM crypto_data
            WHERE timestamp < NOW() - INTERVAL '7 days'
        """)).scalar()
    logger.info(f"📊 Records older than 7 days: {count:,}")
    engine.dispose()
    context["ti"].xcom_push(key="old_record_count", value=count)
    return count


def delete_old_records(**context):
    from sqlalchemy import create_engine, text
    engine = create_engine(DB_URL)
    expected = context["ti"].xcom_pull(
        task_ids="count_old_records", key="old_record_count"
    ) or 0

    if expected == 0:
        logger.info("✅ No records older than 7 days — nothing to delete.")
        engine.dispose()
        return

    with engine.begin() as conn:
        deleted = conn.execute(text("""
            DELETE FROM crypto_data
            WHERE timestamp < NOW() - INTERVAL '7 days'
        """)).rowcount

    logger.info(f"🗑️  Deleted {deleted:,} old records.")
    engine.dispose()


def log_database_stats(**context):
    from sqlalchemy import create_engine, text
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        db_size    = conn.execute(text("SELECT pg_size_pretty(pg_database_size('streamflow_db'))")).scalar()
        live_count = conn.execute(text("SELECT COUNT(*) FROM crypto_data")).scalar()
        oldest_ts  = conn.execute(text("SELECT MIN(timestamp) FROM crypto_data")).scalar()
    logger.info(f"💾 DB size: {db_size} | Live rows: {live_count:,} | Oldest: {oldest_ts}")
    engine.dispose()


def vacuum_table(**context):
    import psycopg2
    conn = psycopg2.connect(
        host="postgres",      # ← postgres not postgres
        port=5432,
        dbname="streamflow_db",
        user="admin",
        password="admin123"
    )
    conn.set_isolation_level(0)
    cur = conn.cursor()
    cur.execute("VACUUM ANALYZE crypto_data;")
    cur.close()
    conn.close()
    logger.info("✅ VACUUM ANALYZE completed.")


with DAG(
    dag_id="streamflow_daily_cleanup",
    description="Delete crypto_data records older than 7 days and vacuum the table",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["streamflow", "maintenance", "cleanup"],
) as dag:

    start    = EmptyOperator(task_id="start")
    end      = EmptyOperator(task_id="end")
    t_check  = PythonOperator(task_id="check_db_connection",  python_callable=check_db_connection)
    t_count  = PythonOperator(task_id="count_old_records",    python_callable=count_old_records)
    t_delete = PythonOperator(task_id="delete_old_records",   python_callable=delete_old_records)
    t_stats  = PythonOperator(task_id="log_database_stats",   python_callable=log_database_stats)
    t_vacuum = PythonOperator(task_id="vacuum_table",         python_callable=vacuum_table)

    start >> t_check >> t_count >> t_delete >> t_stats >> t_vacuum >> end
