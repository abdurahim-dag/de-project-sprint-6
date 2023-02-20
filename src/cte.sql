-- пользователи в группе, с ообшением > 0
with user_group as (
    select distinct lgd.hk_group_id, lum.hk_user_id
    from RAGIMATAMOV_YANDEX_RU__DWH.l_user_message lum
             join RAGIMATAMOV_YANDEX_RU__DWH.l_groups_dialogs lgd on lgd.hk_message_id = lum.hk_message_id
),
-- количество уникальных пользователей в группе, с ообшением > 0
     user_group_messages as (
         select hk_group_id, count(1) as cnt_users_in_group_with_messages
         from user_group
         group by hk_group_id),
-- новые пользователи в группе
     user_group_add as (
         select distinct luga.hk_group_id, luga.hk_user_id
         from RAGIMATAMOV_YANDEX_RU__DWH.l_user_group_activity luga
                  join RAGIMATAMOV_YANDEX_RU__DWH.s_auth_history sah on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
         where sah.event = 'add'
     ),
-- количестов уникальных новых пользователей в группе
     user_group_log as (
         select hk_group_id, count(1) as cnt_added_users
         from user_group_add
         group by hk_group_id
     ),
-- 10 самых старых группах
     groups_old as (
         select hk_group_id
         from RAGIMATAMOV_YANDEX_RU__DWH.h_groups
         order by registration_dt asc
         limit 10
     )

select
    go.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion
from groups_old go
         join user_group_log ugl      on ugl.hk_group_id = go.hk_group_id
         join user_group_messages ugm on ugm.hk_group_id = go.hk_group_id
order by group_conversion desc
