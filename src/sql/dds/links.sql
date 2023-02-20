INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select distinct
    hash(hu.hk_user_id, hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from RAGIMATAMOV_YANDEX_RU__STAGING.dialog_log as dl
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_users as hu on dl.user_id = hu.user_id
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_groups as hg on dl.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from RAGIMATAMOV_YANDEX_RU__DWH.l_user_group_activity);


INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
    hash(hg.hk_group_id,hu.hk_user_id),
    hg.hk_group_id,
    hu.hk_user_id,
    now() as load_dt,
    's3' as load_src
from RAGIMATAMOV_YANDEX_RU__STAGING.groups as g
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_users as hu on g.admin_id = hu.user_id
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from RAGIMATAMOV_YANDEX_RU__DWH.l_admins);

INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_group_id, hk_message_id, load_dt, load_src)
select
    hash(hg.hk_group_id, hd.message_id),
    hg.hk_group_id,
    hd.hk_message_id,
    now() as load_dt,
    's3' as load_src
from RAGIMATAMOV_YANDEX_RU__STAGING.dialogs as d
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_dialogs as hd on d.message_id = hd.message_id
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_groups as hg on d.message_group = hg.group_id
where hash(hg.hk_group_id, hd.message_id) not in (select hk_l_groups_dialogs from RAGIMATAMOV_YANDEX_RU__DWH.l_groups_dialogs)
  and d.message_group is not null;

INSERT INTO RAGIMATAMOV_YANDEX_RU__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
select
    hash(hu.user_id, hd.message_id),
    hu.hk_user_id,
    hd.hk_message_id,
    now() as load_dt,
    's3' as load_src
from RAGIMATAMOV_YANDEX_RU__STAGING.dialogs as d
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_users as hu on hu.user_id = d.message_from
         left join RAGIMATAMOV_YANDEX_RU__DWH.h_dialogs as hd on hd.message_id = d.message_id
where hash(hu.user_id, hd.message_id) not in (select hk_l_user_message from RAGIMATAMOV_YANDEX_RU__DWH.l_user_message);