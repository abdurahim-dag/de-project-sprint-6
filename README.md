# Описание задачи
Чтобы привлечь новых пользователей, маркетологи хотят разместить на сторонних сайтах рекламу сообществ с высокой активностью. Вам нужно определить группы, в которых начала общаться большая часть их участников. В терминологии маркетинга их бы назвали пабликами с высокой конверсией в первое сообщение.

## Необходимо:
1. Перенести из S3 в staging-слой новые данные о входе и выходе пользователей из групп — файл group_log.csv.
2. Создать в слое постоянного хранения таблицы для новых данных.
3. Перенести новые данные из staging-области в слой DDS.
4. Рассчитать конверсионные показатели для десяти самых старых групп:

## Написать запрос и ответить на вопрос бизнеса
1. Напишите SQL-запрос, который выведет по десяти самым старым группам:
   - Хэш-ключ группы hk_group_id
   - Количество новых пользователей группы (event = add). Назовите поле cnt_added_users
   - Количество пользователей группы, которые написали хотя бы одно сообщение. Назовите поле cnt_users_in_group_with_messages
   - Долю пользователей группы, которые начали общаться. Назовите выводимое поле group_conversion
2. Отсортируйте результаты по убыванию значений поля group_conversion.


# Решение

Решение представлено в дагах написанных, для всего спринта и в том числе поставленной задачи. 
## Структура репозитория
- `src/dags/realization/init-dag.py` - инициализация схемы;
- `src/dags/realization/load_staging/load_s3_dag.py` - загрузка в staging;
- `src/dags/realization/load-dds-dag` - загрузка в DDS слой; 
- `src/sql/init`    - sql скрипты инициализации слоёв;
- `src/sql/staging` - sql скрипты загрузки в staging слой;
- `src/sql/dds`     - sql скрипты загрузки в DDS слой;
- `src/cte.sql` - sql запрос ответ бизнесу;
- `Makefile` - порядок запуска решения
```
docker-compose-start:
	docker compose up -d --no-deps --build

add-connection:
	docker exec de-sprint-6 bash -c "airflow connections add \"vertica_dwh\" --conn-json '{\
    \"conn_type\": \"vertica\",\
    \"login\": \"ragimatamov_yandex_ru\",\
    \"password\": \"${password}\",\
    \"host\": \"51.250.75.20\",\
    \"port\": \"5433\",\
    \"schema\": \"dwh\",\
    \"extra\": {\"schema\": \"RAGIMATAMOV_YANDEX_RU__DWH\"}\
}'"

add-provider:
	docker exec de-sprint-6 bash -c "pip install apache-airflow-providers-vertica"
	docker restart de-sprint-6
	docker exec de-sprint-6 bash -c "until curl -s -f -o /dev/null http://localhost:3000/airflow/health; do sleep 5; done;"

add-variable:
	docker exec de-sprint-6 bash -c "airflow variables import /lessons/variables.json"

add-variable-api-key-id:
	docker exec de-sprint-6 airflow variables set s3_access_key_id ${key_id}

add-variable-api-secret-key:
	docker exec de-sprint-6 airflow variables set s3_secret_access_key ${secret_key}


start: docker-compose-start add-provider add-connection add-variable

```
- `docker-compose.yml` - контейнеры, для запуска решения.

## Как запустить контейнер
1. make password=password start - пароль к учётке в Vertica;
2. make key_id=key add-variable-api-key-id - key_id доступа к данным в S3;
3. make secret_key=sceret add-variable-api-secret-key - secret_key доступа к данным в S3;
4. Запустить даги.

После того как запустится контейнеры, вам будут доступны:
- Airflow
	- `localhost:3000/airflow`
- БД
	- `jovyan:jovyan@localhost:15432/de`
