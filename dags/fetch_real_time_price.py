from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

import decimal
import pytz

from sql.ticker import create_ticker_table_sql, upsert_tickers_sql
from utils.ccxt_utils import get_ccxt_ex

CONNECTION_ID = 'crypto_prices_db'
INIT_TABLE_NAME = 'tickers'
COLLECTING_EXCHANGES = ['binance', 'bybit', 'okx', 'mexc']


def check_table_exists():
    mysql_hook = MySqlHook(mysql_conn_id=CONNECTION_ID)
    tables = mysql_hook.get_records(sql=f"SHOW TABLES LIKE '{INIT_TABLE_NAME}'")
    return 'skip_create_table_task' if tables else 'create_table_task'


def fetch_tickers(connection_id, exchange):
    ex = get_ccxt_ex(exchange)
    tickers = ex.fetch_tickers()
    ticker_data = []
    timezone = pytz.timezone('Asia/Singapore')

    for ticker in tickers.values():
        symbol = ticker.get('symbol')
        close_price = ticker.get('close')
        if not symbol or not close_price:
            continue
        timestamp = ticker.get('timestamp')
        if timestamp is None:
            collect_datetime = datetime.now(tz=timezone)
        else:
            collect_datetime = datetime.fromtimestamp(timestamp // 1000).astimezone(tz=timezone)
        base, quote = symbol.split('/')
        if exchange == 'bybit':
            # bybit symbols are in 'ZKF/USDT:USDT' format
            quote = quote.split(':')[0]
        price = decimal.Decimal(str(close_price))
        ticker_data.append(f'("{base}", "{quote}", {price}, "{exchange}", "{collect_datetime}")')
    if ticker_data:
        mysql_hook = MySqlHook(mysql_conn_id=connection_id)
        mysql_hook.run(upsert_tickers_sql.format(new_rows=(','.join(ticker_data))))


def generate_dag(exchange):
    with DAG(
        dag_id=f'Fetch_Real_Time_Price_{exchange}',
        default_args={
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description=f"Fetch real time price from {exchange}",
        schedule=timedelta(seconds=30),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example"],
        max_active_runs=1
    ) as dag:

        check_table_exists_task = BranchPythonOperator(
            task_id='check_table_exists',
            python_callable=check_table_exists,
            provide_context=True,
            dag=dag
        )

        create_table_task = MySqlOperator(
            task_id='create_table_task',
            sql=create_ticker_table_sql,
            mysql_conn_id=CONNECTION_ID,
            dag=dag,
        )

        skip_create_table_task = EmptyOperator(task_id='skip_create_table_task', dag=dag)

        fetch_tickers_task = PythonOperator(
            task_id='fetch_tickers_task',
            python_callable=fetch_tickers,
            dag=dag,
            op_args=[CONNECTION_ID, exchange],
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )

        check_table_exists_task >> [skip_create_table_task, create_table_task]
        skip_create_table_task >> fetch_tickers_task
        create_table_task >> fetch_tickers_task
    return dag


for exchange_name in COLLECTING_EXCHANGES:
    globals()[f"Dynamic_Ticker_DAG_{exchange_name}"] = generate_dag(exchange_name)
