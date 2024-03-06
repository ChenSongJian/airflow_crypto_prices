from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

import pytz

from sql.snapshot import create_snapshot_table_sql, snapshot_price_sql

CONNECTION_ID = 'crypto_prices_db'
INIT_TABLE_NAME = 'snapshot_{year_month}'


def check_table_exists(year_month):
    mysql_hook = MySqlHook(mysql_conn_id=CONNECTION_ID)
    table_name = INIT_TABLE_NAME.format(year_month=year_month)
    tables = mysql_hook.get_records(sql=f"SHOW TABLES LIKE '{table_name}'")
    return 'skip_create_table_task' if tables else 'create_table_task'


with DAG(
    dag_id='Snapshot_Price',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description=f"Snapshot crypto price every 15 minute",
    schedule=timedelta(minutes=15),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
    max_active_runs=1
) as dag:
    now = datetime.now(tz=pytz.timezone('Asia/Singapore'))
    snapshot_datetime = now.strftime("%Y-%m-%d %H:%M:00")
    year_month = now.strftime("%Y_%m")

    check_table_exists_task = BranchPythonOperator(
        task_id='check_table_exists',
        python_callable=check_table_exists,
        provide_context=True,
        op_args=[year_month],
        dag=dag
    )

    skip_create_table_task = EmptyOperator(task_id='skip_create_table_task', dag=dag)

    create_table_task = MySqlOperator(
        task_id='create_table_task',
        sql=create_snapshot_table_sql.format(year_month=year_month),
        mysql_conn_id=CONNECTION_ID,
        dag=dag,
    )

    snapshot_price_task = MySqlOperator(
        task_id='snapshot_price_task',
        sql=snapshot_price_sql.format(year_month=year_month, snapshot_datetime=snapshot_datetime),
        mysql_conn_id=CONNECTION_ID,
        dag=dag,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    check_table_exists_task >> [skip_create_table_task, create_table_task]
    skip_create_table_task >> snapshot_price_task
    create_table_task >> snapshot_price_task
