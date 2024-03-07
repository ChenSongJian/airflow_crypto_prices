from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import logging

from celery_kline_tasks import fetch_kline_task
from sql.kline import upsert_with_kline_sql

CONNECTION_ID = 'crypto_prices_db'
COLLECTING_EXCHANGES = ['binance', 'bybit', 'okx', 'mexc']

logger = logging.getLogger(__name__)


def fetch_kline(connection_id, exchange):
    fetch_kline_task.delay(exchange, connection_id, upsert_with_kline_sql)


def generate_dag(exchange):
    with DAG(
        dag_id=f'Fetch_Kline_Price_{exchange}',
        default_args={
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(seconds=5),
        },
        description=f"Fetch kline price from {exchange}",
        schedule_interval='2,17,32,47 * * * *',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example"],
        max_active_runs=1
    ) as dag:
        fetch_kline_task = PythonOperator(
            task_id='fetch_kline_task',
            python_callable=fetch_kline,
            dag=dag,
            op_args=[CONNECTION_ID, exchange],
            provide_context=True
        )

        end_task = EmptyOperator(task_id='end_task', dag=dag)

        fetch_kline_task >> end_task
    return dag


for exchange_name in COLLECTING_EXCHANGES:
    globals()[f"Dynamic_Kline_DAG_{exchange_name}"] = generate_dag(exchange_name)
