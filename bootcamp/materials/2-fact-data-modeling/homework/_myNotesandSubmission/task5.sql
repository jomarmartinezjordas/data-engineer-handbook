-- task 5: ddl for hosts_cumulated
create table hosts_cumulated (
    host_id text,
    host_activity_datelist date[], -- array of dates with activity
    date date,
    primary key (host_id, date)
);