select customer_count_in_customer_link_table,
       customer_count_in_crm_customer_table,
       case when customer_count_in_crm_customer_table<> customer_count_in_customer_link_table
           then 'Customer Count not consistent between source crm and customer link table'
       else 'passed validation' end as validation_message
       from (
select count(customer_id) as customer_count_in_crm_customer_table,
       (select count(distinct linked_customer_id) from data_warehouse.customer_link) as customer_count_in_customer_link_table

from ac_shopping_crm.customer)
where customer_count_in_crm_customer_table <> customer_count_in_customer_link_table;