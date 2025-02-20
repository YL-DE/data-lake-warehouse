---overlapping with current dim customer table
truncate table staging.dim_customer;
insert into staging.dim_customer
select
customer_id,
first_name,
last_name,
gender,
dob,
registered_at,
last_login_at,
email_address,
mobile,
street_1,
street_2,
suburb,
state,
country,
postcode,
x.created_at,
getdate() as updated_at,
dedupe_number,
(select dim_customer_key from data_warehouse.dim_customer dc where dc.mobile = x.mobile) as dim_customer_key
from
(select
customer_id,
first_name,
last_name,
gender,
(select dob from ac_shopping_crm.customer c3
where c3.mobile =cu.mobile and (c3.dob is not null and trim(c3.dob)<>'')
order by c3.registered_at desc limit 1) as dob,
(select min(registered_at) from  ac_shopping_crm.customer c where c.mobile = cu.mobile) as registered_at,
(select max(last_login_at) from  ac_shopping_crm.customer c where c.mobile = cu.mobile) as last_login_at,
email_address,
mobile,
street_1,
street_2,
suburb,
state,
country,
postcode,
created_at,
row_number() over (partition by mobile order by registered_at desc) as dedupe_number
from ac_shopping_crm.customer cu
where (created_at >='{de_daily_load_data_warehouse_dim_customer}' or updated_at>='{de_daily_load_data_warehouse_dim_customer}')
and mobile in(select mobile from data_warehouse.dim_customer)) x;

--1.1 update to data_warehouse_dim_customer keep the dim_customer_key;
update data_warehouse.dim_customer
set customer_id = sc.customer_id,
    first_name =sc.first_name,
    last_name = sc.last_name,
    gender = sc.gender,
    dob = sc.dob,
    registered_at = sc.registered_at,
    last_login_at =sc.last_login_at,
    email_address = sc.email_address,
    mobile = sc.mobile,
    street_1 = sc.street_1,
    street_2 = sc.street_2,
    suburb = sc.suburb,
    state = sc.state,
    country =sc.country,
    postcode = sc.postcode,
    updated_at = getdate()
from staging.dim_customer sc join data_warehouse.dim_customer dc on sc.dim_customer_key = dc.dim_customer_key
and sc.dedupe_number = 1;

--1.2 update customer_link
delete from data_warehouse.customer_link
where exists(select 1 from staging.dim_customer dc where dc.dim_customer_key = customer_link.dim_customer_key);
insert into data_warehouse.customer_link(dim_customer_key, customer_id, linked_customer_id)
select
dim_customer_key,
(select customer_id from staging.dim_customer dc where dc.dim_customer_key = dc1.dim_customer_key and dc.dedupe_number=1)as customer_id,
customer_id as linked_customer_id
from staging.dim_customer dc1;

--2.1  non-overlapping, based on mobile number
truncate table staging.dim_customer;
insert into staging.dim_customer
select
customer_id,
first_name,
last_name,
gender,
dob,
registered_at,
last_login_at,
email_address,
mobile,
street_1,
street_2,
suburb,
state,
country,
postcode,
x.created_at,
getdate() as updated_at,
dedupe_number,
null as dim_customer_key
from
(select
customer_id,
first_name,
last_name,
gender,
(select dob from ac_shopping_crm.customer c3
where c3.mobile =cu.mobile and (c3.dob is not null and trim(c3.dob)<>'')
order by c3.registered_at desc limit 1) as dob,
(select min(registered_at) from  ac_shopping_crm.customer c where c.mobile = cu.mobile) as registered_at,
(select max(last_login_at) from  ac_shopping_crm.customer c where c.mobile = cu.mobile) as last_login_at,
email_address,
mobile,
street_1,
street_2,
suburb,
state,
country,
postcode,
created_at,
row_number() over (partition by mobile order by registered_at desc) as dedupe_number
from ac_shopping_crm.customer cu
where (created_at >='{de_daily_load_data_warehouse_dim_customer}' or updated_at>='{de_daily_load_data_warehouse_dim_customer}') and mobile not in(select mobile from data_warehouse.dim_customer)) x;

--from staging to dim_customer
insert into data_warehouse.dim_customer
(customer_id, first_name, last_name, gender, dob, registered_at, last_login_at, email_address, mobile, street_1, street_2, suburb, state, country, postcode, created_at, updated_at)
select
customer_id,
first_name,
last_name,
gender,
dob,
registered_at,
last_login_at,
email_address,
mobile,
street_1,
street_2,
suburb,
state,
country,
postcode,
getdate(),
getdate()
from staging.dim_customer
where dedupe_number = 1;

update staging.dim_customer

set dim_customer_key = sc.dim_customer_key

from staging.dim_customer dc join data_warehouse.dim_customer sc on dc.mobile = sc.mobile;

--2.2 update customer_link
insert into data_warehouse.customer_link(
dim_customer_key,
customer_id,
linked_customer_id
)
select
dim_customer_key,
(select customer_id from staging.dim_customer dc1 where dc1.dim_customer_key = dim_customer.dim_customer_key and dc1.dedupe_number=1),
customer_id
from staging.dim_customer;

