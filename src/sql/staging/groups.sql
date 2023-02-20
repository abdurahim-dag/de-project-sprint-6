COPY /*+label(ragiminsertquery)*/ RAGIMATAMOV_YANDEX_RU__STAGING.groups(id, admin_id, group_name, registration_dt, is_private)
FROM LOCAL STDIN
DELIMITER ','
ENCLOSED BY '"'
REJECTED DATA AS TABLE RAGIMATAMOV_YANDEX_RU__STAGING.groups_rej
;
