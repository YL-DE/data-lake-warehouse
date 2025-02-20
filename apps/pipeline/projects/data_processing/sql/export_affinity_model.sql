
unload('select
    o.order_id,
    o.customer_id,
    ol.product_id,
    o.order_datetime,
    date_part(Year, o.order_datetime)::int as order_datetime_year,
    date_part(Month, o.order_datetime)::int as order_datetime_month,
    o.order_datetime::date as order_datetime_day
from ac_shopping_crm.order o
left join ac_shopping_crm.order_line ol on o.order_id = ol.order_id
where o.order_datetime>=''2020-06-01''')

to 's3://ac-shopping-redshift-export/order/'
cleanpath
credentials 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
parquet
partition by (order_datetime_year, order_datetime_month, order_datetime_day)
maxfilesize 5MB;


unload('
select
    product_id,
    brand_name,
    category_level_1,
    category_level_2
from ac_shopping_crm.product')
to 's3://ac-shopping-redshift-export/product/'
cleanpath
credentials 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
parquet
maxfilesize 5MB;
