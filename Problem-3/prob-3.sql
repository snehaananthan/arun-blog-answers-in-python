
##Hive##
create external table orders_sqoop 
stored as avro 
location '/user/snehaananthan/warehouse/retail_stage.db/orders' 
tblproperties('avro.schema.url'='/user/snehaananthan/schemas/orders/orders.avsc');

select * from orders_sqoop 
where order_date in 
(select order_date from 
(select order_date, count(1) as num_orders from orders_sqoop 
group by order_date 
order by num_orders desc limit 1) q); 

SET hive.exec.dynamic.partition=true; 
SET hive.exec.dynamic.partition.mode=nonstrict;

create table orders_avro(
order_id int,  
order_date string, 
order_customer_id int, 
order_status string) 
PARTITIONED BY(order_month string)
STORED as AVRO;

 

insert overwrite table orders_avro partition (order_month) 
select order_id, 
order_date, 
order_customer_id, 
order_status, 
substr(order_date, 1, 7) mnth  
from orders_sqoop;

select * from orders_avro 
where order_date in 
(select order_date from 
(select order_date, count(1) as num_orders 
from orders_avro 
group by order_date 
order by num_orders desc limit 1)); 


insert into table orders_sqoop values(890000, '2018-03-05 16:50:46.956', 456789, 'NEW', 'VERY CONTEMPORARY', 8);