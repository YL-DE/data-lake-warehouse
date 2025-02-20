---insert into dim product from source table, records with product_ids not in destination
---table
insert into data_warehouse.dim_product(
product_id,
seller_id,
product_title,
brand_name,
category_level_1,
category_level_2,
image_url,
is_active,
unit_price,
quantity_for_sale
)
select
    p.product_id,
    (select max(seller_id) from ac_shopping_crm.product_upload pu where pu.product_id = p.product_id) as seller_id,
    product_title,
    brand_name,
    category_level_1,
    category_level_2,
    image_url,
    is_active,
    unit_price,
    quantity_for_sale
from ac_shopping_crm.product p
where p.updated_at>='{de_daily_load_data_warehouse_dim_product}'
and p.product_id not in (select product_id from data_warehouse.dim_product);

---udpate based on values from another table, if product id from source exists in dim product table
update data_warehouse.dim_product
set seller_id = b.seller_id,
product_title =b.product_title,
brand_name=b.brand_name,
category_level_1=b.category_level_1,
category_level_2=b.category_level_2,
image_url=b.image_url,
is_active=b.is_active,
unit_price=b.unit_price,
quantity_for_sale=b.quantity_for_sale
from data_warehouse.dim_product a join (
select
    p.product_id,
    (select max(seller_id) from ac_shopping_crm.product_upload pu where pu.product_id = p.product_id) as seller_id,
    product_title,
    brand_name,
    category_level_1,
    category_level_2,
    image_url,
    is_active,
    unit_price,
    quantity_for_sale
from ac_shopping_crm.product p
where p.updated_at>='{de_daily_load_data_warehouse_dim_product}'
and p.product_id in (select product_id from data_warehouse.dim_product)) b
on a.product_id = b.product_id;