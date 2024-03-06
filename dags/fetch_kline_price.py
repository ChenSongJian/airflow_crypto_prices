from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

import pytz

from crypto_price.kline import fetch_kline
from sql.kline import get_kline_datetime_sql

CONNECTION_ID = 'crypto_prices_db'
COLLECTING_EXCHANGES = ['binance']  #, 'bybit', 'okx', 'kraken', 'mexc', 'fake']  # fake is an example of invalid exchange


def get_kline_datetime(year_month, exchange):
    mysql_hook = MySqlHook(mysql_conn_id=CONNECTION_ID)
    sql = get_kline_datetime_sql.format(year_month=year_month, exchange=exchange)
    kline_datetime = mysql_hook.get_first(sql=sql)[0]
    return kline_datetime


def generate_dag(exchange):
    with DAG(
        dag_id=f'Fetch_Kline_Price_{exchange}',
        default_args={
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description=f"Fetch kline price from {exchange}",
        schedule=timedelta(minutes=1000),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example"],
        max_active_runs=1
    ) as dag:
        now = datetime.now(tz=pytz.timezone('Asia/Singapore'))
        year_month = now.strftime("%Y_%m")

        get_kline_datetime_task = PythonOperator(
            task_id='get_kline_datetime_task',
            python_callable=get_kline_datetime,
            dag=dag,
            op_args=[year_month, exchange]
        )

        fetch_kline_task = PythonOperator(
            task_id='fetch_kline_task',
            python_callable=fetch_kline,
            dag=dag,
            op_args=[CONNECTION_ID, exchange],
            provide_context=True
        )

        get_kline_datetime_task >> fetch_kline_task
    return dag


for exchange_name in COLLECTING_EXCHANGES:
    globals()[f"Dynamic_Kline_DAG_{exchange_name}"] = generate_dag(exchange_name)
