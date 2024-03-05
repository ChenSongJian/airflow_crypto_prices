from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import ccxt
import decimal
import logging
import pytz

logger = logging.getLogger(__name__)

create_ticker_table_sql = """
CREATE TABLE tickers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    base_coin VARCHAR(20) NOT NULL,
    quote_coin VARCHAR(20) NOT NULL,
    price DECIMAL(36, 16) NOT NULL,
    collect_datetime DATETIME NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    UNIQUE KEY idx_unique_ticker (base_coin, quote_coin, exchange),
    INDEX idx_base_coin_ticker (base_coin),
    INDEX idx_quote_coin_ticker (quote_coin),
    INDEX idx_exchange_ticker (exchange),
    INDEX idx_collect_datetime (collect_datetime)
);
"""

upsert_tickers_sql = """
INSERT INTO tickers (base_coin, quote_coin, price, exchange, collect_datetime)
VALUES {new_rows}
ON DUPLICATE KEY UPDATE price = VALUES(price), collect_datetime = VALUES(collect_datetime);
"""


def check_table_exists(connection_id, table_name):
    mysql_hook = MySqlHook(mysql_conn_id=connection_id)
    tables = mysql_hook.get_records(sql=f"SHOW TABLES LIKE '{table_name}'")
    return 'skip_create_table_task' if tables else 'create_table_task'


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
        base, quote = symbol.split('/')
        price = decimal.Decimal(str(close_price))
        collect_datetime = datetime.fromtimestamp(timestamp).astimezone(tz=pytz.timezone('Asia/Singapore'))
        ticker_data.append(f'("{base}", "{quote}", {price}, "{exchange.name}", "{collect_datetime}")')

    if ticker_data:
        mysql_hook = MySqlHook(mysql_conn_id=connection_id)
        logger.info(upsert_tickers_sql.format(new_rows=(','.join(ticker_data))))
        mysql_hook.run(upsert_tickers_sql.format(new_rows=(','.join(ticker_data))))


with DAG(
    dag_id='Fetch_Real_Time_Price',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Fetch real time price across multiple crypto exchanges",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
    max_active_runs=1
    ) as dag:

    check_table_exists_task = BranchPythonOperator(
        task_id='check_table_exists',
        python_callable=check_table_exists,
        provide_context=True,
        dag=dag,
        op_args=['crypto_prices_db', 'tickers']
    )

    create_table_task = MySqlOperator(
        task_id='create_table_task',
        sql=create_ticker_table_sql,
        mysql_conn_id='crypto_prices_db',
        dag=dag,
    )

    skip_create_table_task = EmptyOperator(task_id='skip_create_table_task', dag=dag)

    fetch_tickers_task = PythonOperator(
        task_id='fetch_tickers_task',
        python_callable=fetch_tickers,
        dag=dag,
        op_args=['crypto_prices_db'],
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    check_table_exists_task >> [skip_create_table_task, create_table_task]
    skip_create_table_task >> fetch_tickers_task
    create_table_task >> fetch_tickers_task
