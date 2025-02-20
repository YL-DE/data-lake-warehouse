insert into data_warehouse.dim_device (
device_name, created_at, updated_at

)
select
distinct device,
getdate(),
getdate()
from ac_shopping_crm."order"
where device not in (select device_name from data_warehouse.dim_device);