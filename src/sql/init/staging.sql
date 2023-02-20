--
create table if not exists RAGIMATAMOV_YANDEX_RU__STAGING.group_log(
    group_id integer not null,
    user_id integer not null,
    user_id_from integer null,
    event varchar(200) not null,
    datetime timestamp(0) not null
)
order by user_id, group_id
segmented by hash(user_id) all nodes
partition by datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);

--
create table if not exists RAGIMATAMOV_YANDEX_RU__STAGING.users(
   id integer primary key ENABLED,
   chat_name varchar(200),
   registration_dt timestamp(0),
   country varchar(200),
   age integer
)
order by id
segmented by hash(id) all nodes
partition by registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__STAGING.groups(
    id integer primary key ENABLED,
    admin_id integer,
    group_name varchar(100),
    registration_dt timestamp(6),
    is_private boolean
)
order by id, admin_id
segmented by hash(id) all nodes
partition by registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);

create table if not exists RAGIMATAMOV_YANDEX_RU__STAGING.dialogs(
    message_id integer primary key ENABLED,
    message_from integer null,
    message_to integer null,
    message varchar(1000) null,
    message_ts timestamp(6) null,
    message_group integer null
)
order by message_id
segmented by hash(message_id) all nodes
partition by message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);
