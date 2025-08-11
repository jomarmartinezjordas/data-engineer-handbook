do $$
declare
    current_loop_date date := '2023-01-01'; 
    end_date date := '2023-01-31';

begin
    -- loop through each date from 2023-01-01 to 2023-01-31
    while current_loop_date <= end_date loop
        with last_day_cumulated as (
            -- fetch the last recorded data for the previous day
            select *
            from hosts_cumulated
            where date = current_loop_date - interval '1 day'
        ),

        current_day_data as (
            -- fetch today's activity from the events table
            select 
                host as host_id,
                date_trunc('day', cast(event_time as timestamp)) as today_date
            from events
            where date_trunc('day', cast(event_time as timestamp)) = current_loop_date
            group by host, date_trunc('day', cast(event_time as timestamp))
        ),

        updated_hosts as (
            -- update the activity list for existing hosts
            select 
                ld.host_id,
                case 
                    when cd.today_date is not null then ld.host_activity_datelist || array[cd.today_date]
                    else ld.host_activity_datelist
                end as host_activity_datelist,
                current_loop_date as date
            from last_day_cumulated ld
                left join current_day_data cd on ld.host_id = cd.host_id
        ),

        new_hosts as (
            -- insert new hosts that are not in the previous day's data
            select 
                cd.host_id,
                array[cd.today_date] as host_activity_datelist,
                cd.today_date as date
            from current_day_data cd
                left join last_day_cumulated ld on cd.host_id = ld.host_id
            where ld.host_id is null
        )
        -- insert updated and new records into hosts_cumulated
        insert into hosts_cumulated (host_id, host_activity_datelist, date)
        
        select * from updated_hosts
        union all
        select * from new_hosts;
        
        -- increment the current_loop_date
        current_loop_date := current_loop_date + interval '1 day';
    end loop;
end $$;
