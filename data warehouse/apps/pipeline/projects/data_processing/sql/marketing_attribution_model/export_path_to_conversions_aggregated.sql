UNLOAD ('with non_conversions as (
    select
        i.conversion_time_window_start,
        i.conversion_time_window_end,
        i.domain_userid,
        max(i.collector_tstamp_start) as non_conversion_timestamp
    from staging.tmp_marketing_attribution_sessions i
        left join staging.tmp_marketing_attribution_path_to_conversions c on i.domain_userid = c.domain_userid
    where c.domain_userid is null
        and i.collector_tstamp_start between i.conversion_time_window_start
        and i.conversion_time_window_end
        and i.domain_userid is not null
    group by i.domain_userid,i.conversion_time_window_start, i.conversion_time_window_end, i.domain_userid),

    path_to_non_conversions as (
        select
            conversion_time_window_start,
            conversion_time_window_end,
            domain_userid,
                (select listagg(d.campaign_id, '';'')
                within group (order by d.collector_tstamp_start)
            from staging.tmp_marketing_attribution_sessions d
            where d.domain_userid = c.domain_userid
                and datediff(day, d.collector_tstamp_start, c.non_conversion_timestamp) between 0 and 7
                and d.campaign_id <> coalesce(d.next_campaign_id::varchar, '''')
                ) as path_to_non_conversions
        from non_conversions c),

    path_to_conversions_aggregated as (
        select
            a.conversion_time_window_start,
            a.conversion_time_window_end,
            a.path_to_conversions,
            count(distinct order_id) as total_conversions,
            sum(revenue_amount_exgst) as total_revenue
        from staging.tmp_marketing_attribution_path_to_conversions a
        group by a.path_to_conversions,a.conversion_time_window_start, a.conversion_time_window_end),

    path_to_non_conversions_aggregated as (
        select
            a.conversion_time_window_start,
            a.conversion_time_window_end,
            a.path_to_non_conversions,
            count(distinct domain_userid) as total_non_conversions
        from path_to_non_conversions a
        group by a.path_to_non_conversions,a.conversion_time_window_start, a.conversion_time_window_end, a.path_to_non_conversions
        )

select
    coalesce(c.conversion_time_window_start, n.conversion_time_window_start) as conversion_time_window_start,
    coalesce(c.conversion_time_window_end, n.conversion_time_window_end) as conversion_time_window_end,
    coalesce(c.path_to_conversions, n.path_to_non_conversions) as path_to_conversions,
    coalesce(c.total_conversions, 0) as total_conversions,
    coalesce(n.total_non_conversions, 0) as total_non_conversions,
    coalesce(c.total_revenue, 0) as total_revenue
from path_to_conversions_aggregated c
    full outer join path_to_non_conversions_aggregated n on c.path_to_conversions = n.path_to_non_conversions')
TO '{{ params.s3_object }}'
---TO 's3://dna-redshift-export-stage/marketing_attribution_model/path_to_conversions_aggregated.csv000'
CREDENTIALS 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
CSV
CLEANPATH
parallel off
HEADER;