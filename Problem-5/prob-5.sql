mysql --host=ms.itversity.com --port=3306 \
--user=retail_user --password=itversity

use retail_db;
create table retail_export.products_snehaananthan as select * from products;

use retail_export;
alter table products_snehaananthan add primary key (product_id);
alter table products_snehaananthan add column (product_grade int, product_sentiment varchar(100));

update products_snehaananthan set product_grade = 1  where product_price > 500;
update products_snehaananthan set product_sentiment  = ‘WEAK’  where product_price between 300 and  500;


insert into products_snehaananthan 
  values (1348,2,'something 1','something 2',300.00,'not avaialble',3,'STRONG');
insert into products_snehaananthan 
  values (1349,5,'something 787','something 2',356.00,'not avaialble',3,'STRONG');

insert into products_hive values(1350, 3, 'something 567', 'something 5', 789.00, 'not available', 3, 'WEAK');
insert into products_hive values(1351, 3, 'something 67', 'something 6', 790.00, 'not available', 4, 'MEDIUM');



create database problem5_snehaananthan;
create table products_hive(product_id int, product_category_id int, product_name string, product_description string, product_price float, product_imaage string, 
product_grade int,  product_sentiment string);


select * from products_snehaananthan order by product_id desc limit 10;
select * from products_hive order by product_id desc limit 10;

create table products_external_snehaananthan(product_id int(11) primary Key, product_grade int(11), product_category_id int(11), product_name varchar(100), 
product_description varchar(100), product_price float, product_impage varchar(500), product_sentiment varchar(100));


select product_id, count(1) as number 
from products_hive 
group by product_id 
having number > 1 
order by product_id; 

select * from products_hive where product_id = 1000;

select product_id from products_external_snehaananthan 
order by product_id desc limit 100;

select count(1) from products_external_snehaananthan;