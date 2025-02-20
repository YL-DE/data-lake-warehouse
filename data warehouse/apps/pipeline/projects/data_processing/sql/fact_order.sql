truncate staging.fact_order;

insert into staging.fact_order (
dim_date_key,
dim_customer_key,
dim_device_key,
dim_order_status_key,
order_id,
total_product_count,
total_revenue,
total_cost,
is_current,
created_at,
updated_at
)
select
d.dim_date_key,
dc.dim_customer_key,
de.dim_device_key,
os.dim_order_status_key,
o.order_id,
sum(ol.quantity) as total_product_count,
sum(revenue) as total_revenue,
sum(cost) as total_cost,
1 as is_current,
getdate(),
getdate()
from ac_shopping_crm."order" o
left join data_warehouse.dim_date d on o.order_datetime::date = d.full_date
left join data_warehouse.customer_link cl on o.customer_id = cl.linked_customer_id
left join data_warehouse.dim_customer dc on dc.customer_id = cl.customer_id
left join data_warehouse.dim_device de on de.device_name = o.device
left join data_warehouse.dim_order_status os on os.order_status = o.order_status
left join ac_shopping_crm.order_line ol on o.order_id =ol.order_id
where d.full_date >= dateadd(day,-14,getdate()::date)
group by
d.dim_date_key,
dc.dim_customer_key,
de.dim_device_key,
os.dim_order_status_key,
o.order_id,
is_current,
getdate(),
getdate();

delete
from data_warehouse.fact_order
where exists(select 1 from staging.fact_order where order_id = fact_order.order_id);

insert into data_warehouse.fact_order
select * from staging.fact_order;