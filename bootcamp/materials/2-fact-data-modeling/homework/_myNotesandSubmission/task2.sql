-- task 2: ddl for user_devices_cumulated
drop table user_devices_cumulated;

create table user_devices_cumulated (
    user_id text,
    browser_type text,
    device_activity_datelist date[], -- array of active dates per browser type
    date date,
    primary key (user_id, browser_type, date)
);