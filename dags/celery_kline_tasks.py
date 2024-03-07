from airflow.hooks.base import BaseHook
from celery import Celery
from datetime import datetime, timedelta
from mysql.connector.connection import MySQLConnection

import ccxt
import decimal
import logging
import pytz
import time
import traceback

from utils.ccxt_utils import get_ccxt_ex

logger = logging.getLogger(__name__)


app = Celery('celery_kline_tasks', broker='redis://localhost')


@app.task
def fetch_kline_task(exchange, connection_id, upsert_with_kline_sql):
    ex = get_ccxt_ex(exchange)
    timezone = pytz.timezone('Asia/Singapore')
    now = datetime.now(tz=timezone)
    kline_datetime = now - timedelta(minutes=now.minute % 15, seconds=now.second, microseconds=now.microsecond)
    kline_timestamp = int(kline_datetime.timestamp()) * 1000
    logger.info(f'collecting kline for Exchange {exchange}, datetime = {kline_datetime}')
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
                time.sleep(0.1)
                price = decimal.Decimal(str(kline[0][4]))
                kline_list.append(f'("{base}", "{quote}", {price}, "{kline_datetime}", "{exchange}", true)')
        except ccxt.errors.BadSymbol:
            continue
        except Exception:
            logger.error(traceback.format_exc())

    logger.info(f'Updating {len(kline_list)} {exchange} ticker prices into kline prices')
    if kline_list:
        connection_info = BaseHook.get_connection(conn_id=connection_id)
        connection = MySQLConnection(user=connection_info.login, host=connection_info.host,
                                     port=connection_info.port, database=connection_info.schema)
        cursor = connection.cursor()
        year_month = kline_datetime.strftime("%Y_%m")
        cursor.execute(upsert_with_kline_sql.format(year_month=year_month, new_rows=(','.join(kline_list))))
        connection.commit()
        cursor.close()
        connection.close()
