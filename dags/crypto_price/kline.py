from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

import ccxt
import decimal
import logging
import pytz
import traceback

from sql.kline import upsert_with_kline_sql, upsert_without_kline_sql

logger = logging.getLogger(__name__)


def fetch_kline(connection_id, exchange, **context):
    kline_datetime_str = context['ti'].xcom_pull(task_ids='get_kline_datetime_task')
    if not kline_datetime_str:
        return
    timezone = pytz.timezone('Asia/Singapore')
    kline_datetime = datetime.strptime(kline_datetime_str, "%Y-%m-%d %H:%M:%S").astimezone(tz=timezone)
    kline_timestamp = int(kline_datetime.timestamp()) * 1000
    try:
        ex = eval(f'ccxt.{exchange}()')
        ex.enableRateLimit = True
    except Exception as e:
        raise Exception(f'Exchange {exchange} not supported!')
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
    mysql_hook = MySqlHook(mysql_conn_id=connection_id)
    year_month = kline_datetime.strftime("%Y_%m")
    if kline_list:
        mysql_hook.run(upsert_with_kline_sql.format(year_month=year_month, new_rows=(','.join(kline_list))))

    mysql_hook.run(upsert_without_kline_sql.format(
        year_month=year_month, snapshot_datetime=kline_datetime, exchange=exchange))
