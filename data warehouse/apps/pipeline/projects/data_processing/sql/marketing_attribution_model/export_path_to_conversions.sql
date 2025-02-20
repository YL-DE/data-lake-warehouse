UNLOAD ('
        select
        conversion_time_window_start,
        conversion_time_window_end,
        domain_userid,
        order_id,
        revenue_amount_exgst,
        path_to_conversions
        from staging.tmp_marketing_attribution_path_to_conversions')
TO '{{ params.s3_object }}'
CREDENTIALS 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
CSV
PARALLEL OFF
CLEANPATH
HEADER;