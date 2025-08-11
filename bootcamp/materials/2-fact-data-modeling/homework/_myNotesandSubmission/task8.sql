-- task 8: incremental query for host_activity_reduced
do $$
declare
    current_date date := date('2023-01-01'); -- this should be parameterized to the date you're processing

begin
    with today_events as (
        select
            host,
            count(1) as daily_hits,
            count(distinct user_id) as daily_unique_visitors
        from events
        where date_trunc('day', cast(event_time as timestamp)) = current_date
        group by host
    ),

    existing_month as (
        select *
        from host_activity_reduced
        where month = date_trunc('month', current_date)
    ),

    combined_data as (
        select
            coalesce(te.host, em.host) as host,
            date_trunc('month', current_date) as month,
            case
                when em.hit_array is not null then
                    em.hit_array || array[coalesce(te.daily_hits, 0)]
                else
                    array_fill(0, array[extract('day' from current_date - date_trunc('month', current_date))::integer])
                    || array[coalesce(te.daily_hits, 0)]
            end as hit_array,
            case
                when em.unique_visitors is not null then
                    em.unique_visitors || array[coalesce(te.daily_unique_visitors, 0)]
                else
                    array_fill(0, array[extract('day' from current_date - date_trunc('month', current_date))::integer])
                    || array[coalesce(te.daily_unique_visitors, 0)]
            end as unique_visitors
        from today_events te
        full outer join existing_month em
        on te.host = em.host
    )

    insert into host_activity_reduced (host, month, hit_array, unique_visitors)
    
    select host, month, hit_array, unique_visitors
    from combined_data
    on conflict (month, host)
    do update set
        hit_array = excluded.hit_array,
        unique_visitors = excluded.unique_visitors;
end $$;