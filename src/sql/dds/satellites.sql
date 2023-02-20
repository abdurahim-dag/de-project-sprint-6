INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
select
    lua.hk_l_user_group_activity,
    hu.hk_user_id,
    dl.event,
    dl.datetime,
    now() as load_dt,
    's3' as load_src
from RAGIMATAMOV_YANDEX_RU__STAGING.dialog_log as dl
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_users as hu on dl.user_id = hu.user_id
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_groups as hg on dl.group_id = hg.group_id
         left join RAGIMATAMOV_YANDEX_RU__DWH.l_user_group_activity lua on lua.hk_group_id = hg.hk_group_id and lua.hk_user_id = hu.hk_user_id;


INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
       True as is_admin,
       hg.registration_dt,
       now() as load_dt,
       's3' as load_src
from RAGIMATAMOV_YANDEX_RU__DWH.l_admins as la
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;

INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
select hu.hk_user_id,
       u.chat_name,
       now() as load_dt,
       's3' as load_src
from RAGIMATAMOV_YANDEX_RU__DWH.h_users as hu
         left join RAGIMATAMOV_YANDEX_RU__STAGING.users as u on u.id=hu.user_id;

INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
select hu.hk_user_id,
       u.country,
       u.age,
       now() as load_dt,
       's3' as load_src
from RAGIMATAMOV_YANDEX_RU__DWH.h_users as hu
         left join RAGIMATAMOV_YANDEX_RU__STAGING.users as u on u.id=hu.user_id;

INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
select hg.hk_group_id,
       g.group_name,
       now() as load_dt,
       's3' as load_src
from RAGIMATAMOV_YANDEX_RU__DWH.h_groups as hg
         left join RAGIMATAMOV_YANDEX_RU__STAGING.groups as g on g.id = hg.group_id;

INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
select hg.hk_group_id,
       g.is_private,
       now() as load_dt,
       's3' as load_src
from RAGIMATAMOV_YANDEX_RU__DWH.h_groups as hg
         left join RAGIMATAMOV_YANDEX_RU__STAGING.groups as g on g.id = hg.group_id;

INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
select hd.hk_message_id,
       d.message,
       d.message_from,
       d.message_to,
       now() as load_dt,
       's3' as load_src
from RAGIMATAMOV_YANDEX_RU__DWH.h_dialogs as hd
         left join RAGIMATAMOV_YANDEX_RU__STAGING.dialogs as d on d.message_id = hd.message_id;
