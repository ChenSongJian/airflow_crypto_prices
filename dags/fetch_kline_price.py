from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta

import ccxt
import decimal
import logging
import traceback
import pytz

from sql.kline import get_kline_datetime_sql, upsert_with_kline_sql
from utils.ccxt import get_ccxt_ex

CONNECTION_ID = 'crypto_prices_db'
COLLECTING_EXCHANGES = ['binance']  #, 'bybit', 'okx', 'kraken', 'mexc', 'fake']  # fake is an example of invalid exchange

logger = logging.getLogger(__name__)


def get_kline_datetime(year_month, exchange):
    mysql_hook = MySqlHook(mysql_conn_id=CONNECTION_ID)
    sql = get_kline_datetime_sql.format(year_month=year_month, exchange=exchange)
    kline_datetime = mysql_hook.get_first(sql=sql)[0]
    return kline_datetime


def fetch_kline(connection_id, exchange):
    timezone = pytz.timezone('Asia/Singapore')
    now = datetime.now(tz=timezone)
    kline_datetime = now - timedelta(minutes=15 + now.minute % 15, seconds=now.second, microseconds=now.microsecond)
    kline_timestamp = int(kline_datetime.timestamp()) * 1000
    logger.info(f'collecting kline for Exchange {exchange}, datetime = {kline_datetime}')
    ex = get_ccxt_ex(exchange)
    tickers = ex.fetch_tickers()
    if not tickers:
        logger.info(f'No available symbols in exchange {exchange}')
        return
    kline_list = []
    for symbol in tickers.keys():
        if '/' not in symbol:
            # some symbol like "AXSBIDR" does not contain and returning bad symbol error
            continue
        base, quote = symbol.split('/')
        if exchange == 'bybit':
            quote = quote.split(':')[0]
        try:
            kline = ex.fetch_ohlcv(symbol=symbol, timeframe='15m', since=kline_timestamp, limit=1)
            if kline:
                price = decimal.Decimal(str(kline[0][4]))
                kline_list.append(f'("{base}", "{quote}", {price}, "{kline_datetime}", "{exchange}", true)')
        except ccxt.errors.BadSymbol:
            continue
        except Exception:
            logger.error(traceback.format_exc())

    logger.info(f'Updating {len(kline_list)} ticker prices into kline prices')
    if kline_list:
        mysql_hook = MySqlHook(mysql_conn_id=connection_id)
        year_month = kline_datetime.strftime("%Y_%m")
        mysql_hook.run(upsert_with_kline_sql.format(year_month=year_month, new_rows=(','.join(kline_list))))


def generate_dag(exchange):
    with DAG(
        dag_id=f'Fetch_Kline_Price_{exchange}',
        default_args={
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description=f"Fetch kline price from {exchange}",
        schedule=timedelta(minutes=15),
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
