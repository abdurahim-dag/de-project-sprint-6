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
