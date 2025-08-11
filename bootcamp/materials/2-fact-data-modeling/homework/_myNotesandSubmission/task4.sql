-- task 4: datelist_int generation query
with starter as (
    select udc.device_activity_datelist @> array [date(d.valid_date)]   as is_active,
           d.valid_date,
           extract(day from date('2023-01-31') - d.valid_date) as days_since,
           udc.user_id,
           udc.browser_type
    from user_devices_cumulated udc
             cross join
         (select generate_series('2022-12-31', '2023-01-31', interval '1 day') as valid_date) as d
    where date = date('2023-01-31')
),

bits as (
    select 
        user_id,
        browser_type,
        sum(case
                when is_active then (1 << days_since)
                else 0 end)::bit(32) as datelist_int,
        date('2023-01-31') as date
    from starter
    group by user_id, browser_type
     )
     
select * 
from bits;