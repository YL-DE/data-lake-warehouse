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
updated_at,
cancellation_date
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
getdate(),
case when o.order_status = 'Cancelled' then o.updated_at else null end as cancellation_date
from ac_shopping_crm."order" o
left join data_warehouse.dim_date d on o.order_datetime::date = d.full_date
left join data_warehouse.customer_link cl on o.customer_id = cl.linked_customer_id
left join data_warehouse.dim_customer dc on dc.customer_id = cl.customer_id
left join data_warehouse.dim_device de on de.device_name = o.device
left join data_warehouse.dim_order_status os on os.order_status = o.order_status
left join ac_shopping_crm.order_line ol on o.order_id =ol.order_id
where o.updated_at >= dateadd(day,-14,getdate()::date)
group by
d.dim_date_key,
dc.dim_customer_key,
de.dim_device_key,
os.dim_order_status_key,
o.order_id,
is_current,
getdate(),
getdate(),
cancellation_date;

---insert into fact order with non existing order ids
insert into data_warehouse.fact_order
select *
from staging.fact_order s
where not exists (select 1 from data_warehouse.fact_order fo where fo.order_id = s.order_id );

----use scd type1 to overwrite order ids exisiting in staging fact order table, but new status not equal to "Cancelled"
update
data_warehouse.fact_order
set dim_date_key = s.dim_date_key,
dim_customer_key = s.dim_customer_key,
dim_device_key= s.dim_device_key,
dim_order_status_key= s.dim_order_status_key,
total_product_count= s.total_product_count,
total_revenue= s.total_revenue,
total_cost= s.total_cost,
is_current= s.is_current,
created_at = getdate(),
updated_at = getdate()
from staging.fact_order s
where s.order_id = fact_order.order_id
and s.dim_order_status_key<>
    (select dim_order_status_key from data_warehouse.dim_order_status where order_status = 'Cancelled');


----use scd type2 to generate a new version, and expired the old version, for order ids existing in staging fact order and with status equal to cancelled

update
data_warehouse.fact_order
set is_current = 0,
cancellation_date = s.cancellation_date
from staging.fact_order s
where s.order_id = fact_order.order_id and s.dim_order_status_key =
    (select dim_order_status_key from data_warehouse.dim_order_status where order_status = 'Cancelled');

insert into data_warehouse.fact_order (
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
updated_at,
cancellation_date
)
select
dim_date_key,
dim_customer_key,
dim_device_key,
dim_order_status_key,
order_id,
total_product_count,
total_revenue,
total_cost,
1 as is_currrent,
created_at,
updated_at,
cancellation_date
from staging.fact_order s
where exists(select 1 from data_warehouse.fact_order f where f.order_id = s.order_id)
and s.dim_order_status_key =
    (select dim_order_status_key from data_warehouse.dim_order_status where order_status = 'Cancelled');