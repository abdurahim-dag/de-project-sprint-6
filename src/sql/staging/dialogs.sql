COPY /*+label(ragiminsertquery)*/ RAGIMATAMOV_YANDEX_RU__STAGING.dialogs(message_id,message_ts,message_from,message_to,message,message_group)
FROM LOCAL STDIN
DELIMITER ','
ENCLOSED BY '"'
NO ESCAPE
REJECTED DATA AS TABLE RAGIMATAMOV_YANDEX_RU__STAGING.dialogs_rej
;
