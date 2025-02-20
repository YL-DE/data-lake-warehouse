select device_count_in_crm_order,
       device_count_in_dim_device,
       case when device_count_in_crm_order<> device_count_in_dim_device
           then 'Device Count not consistent between source crm and dim device table'
       else 'passed validation' end as validation_message
       from (
select count(distinct device) as device_count_in_crm_order,
       (select count(distinct device_name) from data_warehouse.dim_device) as device_count_in_dim_device

from ac_shopping_crm."order")
where device_count_in_crm_order <> device_count_in_dim_device;