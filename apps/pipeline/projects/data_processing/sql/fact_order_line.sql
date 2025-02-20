truncate staging.fact_order_line;
insert into staging.fact_order_line (
dim_customer_key,
dim_product_key,
dim_seller_key,
dim_date_key,
order_id,
product_sale_count,
sale_price,
cost,
revenue
)
select
dc.dim_customer_key,
dp.dim_product_key,
(select ds.dim_seller_key from product_upload pu
join data_warehouse.dim_seller ds on ds.seller_id = pu.seller_id
where pu.product_id = ol.product_id limit 1) as dim_seller_key,
d.dim_date_key,
o.order_id,
ol.quantity as product_sale_count,
ol.sale_price,
ol.cost,
ol.revenue
from ac_shopping_crm.order_line ol
left join data_warehouse.dim_product dp on dp.product_id = ol.product_id
left join ac_shopping_crm."order" o on ol.order_id = o.order_id
left join data_warehouse.dim_date d on o.order_datetime::date = d.full_date
left join data_warehouse.customer_link cl on o.customer_id = cl.linked_customer_id
left join data_warehouse.dim_customer dc on dc.customer_id = cl.customer_id
where d.full_date >= dateadd(day,-14,getdate()::date)
;

delete
from data_warehouse.fact_order_line
where exists(select 1 from staging.fact_order_line where staging.fact_order_line.order_id = fact_order_line.order_id);

insert into  data_warehouse.fact_order_line (dim_customer_key,
dim_product_key,
dim_seller_key,
dim_date_key,
order_id,
product_sale_count,
sale_price,
cost,
revenue,
created_at,
updated_at,
is_new_customer)
select
dim_customer_key,
dim_product_key,
dim_seller_key,
dim_date_key,
order_id,
product_sale_count,
sale_price,
cost,
revenue,
created_at,
updated_at,
case when exists(select 1 from data_warehouse.fact_order_line ol where ol.dim_customer_key
    =s.dim_customer_key and ol.dim_date_key < s.dim_date_key)
then false else true end as is_new_customer

from staging.fact_order_line s;