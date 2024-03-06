from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

import ccxt
import decimal
import pytz

from sql.ticker import upsert_tickers_sql


def fetch_tickers(connection_id, exchange):
    try:
        ex = eval(f'ccxt.{exchange}()')
    except Exception as e:
        raise Exception(f'Exchange {exchange} not supported!')

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
