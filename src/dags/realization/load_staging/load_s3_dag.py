from realization.logger import logger
from airflow.models.connection import Connection
import pathlib
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.models.variable import Variable

import io
from realization.load_staging.storage import Workflow
from realization.load_staging.storage import JsonFileStorage
import mmap
import boto3
import vertica_python

from airflow.decorators import task
BATCH_SIZE = 400000
DOWNLOAD_TO = '/data'
BUCKET = 'sprint6'

CONNECTION_DWH = 'vertica_dwh'

VARIABLE_KEY_ID = 's3_access_key_id'
VARIABLE_SECRET_ACCESS_KEY = 's3_secret_access_key'
VARIABLE_SQL_PATH = 'sql_load_staging'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        'load-s3-to-staging',
        catchup=False,
        default_args=args,
        description='Download data from s3 to staging.',
        is_paused_upon_creation=True,
        schedule_interval='@daily',
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        tags=['load', 'staging']
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task()
    def download(file_name: str, ds) -> str:
        """Функция загрузки файлов из S3."""
        dfmt = pendulum.from_format(ds, 'YYYY-MM-DD').format('DD_MM_YYYY')
        file_path = f"{DOWNLOAD_TO}/{dfmt}_{file_name}"

        if not pathlib.Path(file_path).exists():
            session = boto3.session.Session()
            s3_client = session.client(
                service_name='s3',
                endpoint_url='https://storage.yandexcloud.net',
                aws_access_key_id=Variable.get(VARIABLE_KEY_ID),
                aws_secret_access_key=Variable.get(VARIABLE_SECRET_ACCESS_KEY),
            )
            s3_client.download_file(
                Bucket=BUCKET,
                Key=file_name,
                Filename=file_path,
            )
            logger.info('File %s uploaded.', file_name)
        else:
            logger.info('File %s previously uploaded.', file_name)

        return file_path

    @task()
    def staging_load(file_path: str, sql_name: str) -> str:
        """Функция загрузки файлов из S3."""
        sql_dir = Variable.get(VARIABLE_SQL_PATH)
        query = open(f"{sql_dir}/{sql_name}").read()

        conn = Connection.get_connection_from_secrets(CONNECTION_DWH)
        logger.info('Using connection ID %s for task execution.', conn.conn_id)
        conn_info = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "database": conn.schema,
            'autocommit': False,
        }
        if not conn.port:
            conn_info["port"] = 5433
        else:
            conn_info["port"] = int(conn.port)

        with vertica_python.connect(**conn_info) as conn:
            curs = conn.cursor()

            wf_storage = JsonFileStorage(
                path_to_file=f"{DOWNLOAD_TO}/{sql_name[:-4]}.staging.state",
                etl_key=f"{file_path}",
            )
            wf_setting: Workflow = wf_storage.retrieve_state()
            with open(file_path, mode='r+b') as s3_file:
                with mmap.mmap(s3_file.fileno(), length=0, access=mmap.ACCESS_READ) as m:
                    head = m.readline()
                    logger.info('Head is: %s', head)
                    m.seek(wf_setting.id)
                    logger.info('Seek to: %s', wf_setting.id)

                    while True:
                        i = 0
                        file_in_memory = io.BytesIO()
                        while True:
                            line = m.readline()
                            i += 1
                            file_in_memory.write(line)
                            if not line or i == BATCH_SIZE:
                                break

                        wf_setting.id = m.tell()
                        logger.info('Curren position: %s', wf_setting.id)

                        if i != 0:
                            file_in_memory.seek(0)
                            curs.execute(query, copy_stdin=file_in_memory, buffer_size=65536)
                            conn.commit()
                            file_in_memory.close()
                            logger.info('Rows loaded: %s', i)

                        wf_storage.save_state(wf_setting)

                        if not line:
                            break

    files = [
        ('users.csv', 'users.sql'),
        ('groups.csv', 'groups.sql'),
        ('dialogs.csv', 'dialogs.sql'),
        ('group_log.csv', 'group_log.sql')
    ]

    groups = []
    for file, sql in files:

        with TaskGroup(group_id=file[:-4]) as tg:

            t_download = download(
                file_name=file,
                ds='{{ds}}'
            )

            t_staging_load = staging_load(
                file_path=t_download,
                sql_name=sql
            )

        groups.append(tg)

    start >> [tg for tg in groups] >> end