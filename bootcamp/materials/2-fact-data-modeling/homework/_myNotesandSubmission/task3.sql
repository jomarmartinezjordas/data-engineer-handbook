-- task 3: cumulative query for user_devices_cumulated
insert into user_devices_cumulated

with yesterday as (
    select *
    from user_devices_cumulated
    where date = date('2023-01-30')
),

today as (
    select
        e.user_id,
        d.browser_type,
        date_trunc('day', cast(e.event_time as timestamp)) as today_date,
        count(1) as num_events 
    from events e join devices d on e.device_id = d.device_id
    where 
            date_trunc('day', cast(event_time as timestamp)) = date('2023-01-31')
        and e.user_id is not null
    group by 1, 2, 3
)

select
    coalesce(t.user_id, y.user_id) as user_id,
    coalesce(t.browser_type, y.browser_type) as browser_type,
    coalesce(y.device_activity_datelist, array[]::date[])
    || case when
                t.user_id is not null
                then array[t.today_date]
                else array[]::date[] 
                end as device_activity_datelist,
    coalesce(t.today_date, y.date + interval '1 day') as date
from yesterday y
full outer join today t
    on y.user_id = t.user_id and y.browser_type = t.browser_type;