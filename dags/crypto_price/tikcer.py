from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import ccxt
import decimal
import pytz

from sql.ticker import upsert_tickers_sql


def fetch_tickers(connection_id):
    exchange = ccxt.binance()
    tickers = exchange.fetch_tickers()
    ticker_data = []
    for ticker in tickers.values():
        symbol = ticker.get('symbol')
        close_price = ticker.get('close')
        timestamp = ticker.get('timestamp', 0) // 1000
        if not symbol or not close_price or not timestamp:
            continue
        if timestamp < int(datetime.now().timestamp()) - 15 * 60:
            continue
        base, quote = symbol.split('/')
        price = decimal.Decimal(str(close_price))
        collect_datetime = datetime.fromtimestamp(timestamp).astimezone(tz=pytz.timezone('Asia/Singapore'))
        ticker_data.append(f'("{base}", "{quote}", {price}, "{exchange.name}", "{collect_datetime}")')

    if ticker_data:
        mysql_hook = MySqlHook(mysql_conn_id=connection_id)
        mysql_hook.run(upsert_tickers_sql.format(new_rows=(','.join(ticker_data))))
