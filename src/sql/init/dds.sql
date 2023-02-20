-- HUBS *-*-*-*

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime(0),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id      int null,
    registration_dt datetime(6) null,
    load_dt datetime null,
    load_src varchar(20) null
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id integer null,
    message_ts timestamp(6) null,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- LINKS *-*-*-*

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.l_user_group_activity(
    hk_l_user_group_activity bigint primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_user REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_users (hk_user_id),
    hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_groups(hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.l_user_message
(
    hk_l_user_message bigint primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_users (hk_user_id),
    hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_dialogs (hk_message_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs bigint primary key,
    hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_groups(hk_group_id),
    hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_dialogs (hk_message_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.l_admins
(
    hk_l_admin_id bigint primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_admin_user REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_users (hk_user_id),
    hk_group_id bigint not null CONSTRAINT fk_l_admin_group REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_groups(hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- SATELLITES *-*-*-*

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.s_auth_history(
    hk_l_user_group_activity bigint not null CONSTRAINT s_auth_history_l_user_group_activity REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.l_user_group_activity(hk_l_user_group_activity),
    user_id_from integer null,
    event varchar(200) not null,
    event_dt timestamp(0) not null,
    load_dt datetime,
    load_src varchar(20)
)
order by event_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.s_admins
(
    hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.l_admins (hk_l_admin_id),
    is_admin boolean,
    admin_from datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.s_user_chatinfo
(
    hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_users(hk_user_id),
    chat_name varchar(200),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.s_user_socdem
(
    hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_users(hk_user_id),
    country varchar(200),
    age integer,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.s_group_name
(
    hk_group_id bigint not null CONSTRAINT fk_s_group_name_group REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_groups(hk_group_id),
    group_name varchar(100),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.s_group_private_status
(
    hk_group_id bigint not null CONSTRAINT fk_s_group_private_status_group REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_groups(hk_group_id),
    is_private boolean,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__DWH.s_dialog_info
(
    hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialog REFERENCES RAGIMATAMOV_YANDEX_RU__DWH.h_dialogs(hk_message_id),
    message varchar(1000),
    message_from integer,
    message_to   integer,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
