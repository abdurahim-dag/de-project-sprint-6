"""Даг инициализирующий схему DWH."""
import pendulum
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow import DAG
from airflow.models.variable import Variable
from airflow.decorators import task
from realization.logger import logger


CONNECTION_DWH = 'vertica_dwh'
sql_staging = '{{ var.value.sql_init_staging }}'
sql_dds = '{{ var.value.sql_init_dds }}'


args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        'schema-init',
        catchup=False,
        default_args=args,
        description='Initialize schema dag',
        is_paused_upon_creation=True,
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        schedule_interval='@once',
        tags=['init', 'schema', 'ddl']
) as dag:

    @task()
    def schema_init(conn_id: str, file: str):
        cur = VerticaHook(conn_id).get_cursor()
        sql = open(file, encoding='utf-8').read()
        cur.execute(sql)
        result = cur.fetchall()
        logger.info(result)

    staging_init = schema_init(CONNECTION_DWH, sql_staging)
    dds_init = schema_init(CONNECTION_DWH, sql_dds)
    staging_init >> dds_init