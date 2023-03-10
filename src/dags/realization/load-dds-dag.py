"""Даг прогрузки слоя DDS в DWH."""
import pendulum
from airflow.providers.vertica.operators.vertica import VerticaOperator

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from airflow.decorators import task
from realization.logger import logger


connection_dwh = 'vertica_dwh'
sql_dir = Variable.get('sql_load_dds')


args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        'load-dds',
        catchup=False,
        default_args=args,
        description='Load dds from staging layer.',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        schedule_interval='@daily',
        tags=['load', 'dds']
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    files = (
        'hubs.sql',
        'links.sql',
        'satellites.sql',
    )

    tasks = []
    for file in files:
        t_query = VerticaOperator(
            task_id=file[:-4],
            sql=open(f"{sql_dir}/{file}", encoding='utf8').read(),
            vertica_conn_id=connection_dwh,
            dag=dag
        )
        tasks.append(t_query)

    start >> tasks[0] >> tasks[1] >> tasks[2] >> end
