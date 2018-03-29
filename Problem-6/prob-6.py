sqoop import-all-tables --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
-m 4 \
--hive-import \
--hive-database problem6_snehaananthan \
--exclude-tables order_items_nopk,orders \
--outdir java

sqoop import-all-tables --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
-m 4 \
--warehouse-dir /user/snehaananthan/problem6 \
--exclude-tables order_items_nopk,orders \
--outdir java


sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
-m 2 \
--table orders \
--map-column-hive order_date=string \
--create-hive-table \
--hive-import \
--hive-table problem6_snehaananthan.orders \
--outdir java

sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
-m 2 \
--table orders \
--target-dir /user/snehaananthan/problem6/orders \
--map-column-java order_date=String \
--outdir java 


###################################################################################
#Using Spark-SQL
###################################################################################
pyspark --master yarn \
--num-executors 2 \
--executor-memory 1G \
--total-executor-cores 4 \
--packages "com.databricks:spark-avro_2.10:2.0.1" \
--conf spark.ui.port=12999

ranks_DF = sqlContext.sql("select d.department_id, p.*, dense_rank() over (partition by d.department_id order by p.product_price desc) as rank \
from problem6_snehaananthan.products p join problem6_snehaananthan.categories c \
on p.product_category_id = c.category_id \
join problem6_snehaananthan.departments d \
on d.department_id = c.category_department_id \
order by d.department_id, rank desc")

ranks_DF.show(100)

top10Customers_DF = sqlContext.sql("select * from \
(select c.customer_id, c.customer_fname,  \
count(distinct oi.order_item_product_id) as num_of_products, \
rank() over (partition by count(distinct oi.order_item_product_id) order by c.customer_id) as rank  \
from problem6_snehaananthan.customers c join problem6_snehaananthan.orders o \
on c.customer_id = o.order_customer_id \
join problem6_snehaananthan.order_items oi \
on oi.order_item_order_id = o.order_id \
group by c.customer_id, c.customer_fname \
order by num_of_products desc, c.customer_id) q \
where q.rank=1 limit 10")

top10Customers_DF.show()

ranks_less_than_100_DF = sqlContext.sql("select c.category_department_id, p.*, \
rank() over (partition by c.category_department_id order by p.product_price) as rank \
from problem6_snehaananthan.products p join problem6_snehaananthan.categories c \
on p.product_category_id = c.category_id \
where p.product_price < 100.00 \
order by c.category_department_id, rank desc")

ranks_less_than_100_DF.show()

top10Customers_DF.registerTempTable("top10Customers")

top10Customers_less_than_100_DF = sqlContext.sql("select p.* \
from top10Customers t \
join problem6_snehaananthan.orders o on o.order_customer_id = t.customer_id \
join problem6_snehaananthan.order_items oi on oi.order_item_order_id = o.order_id \
join problem6_snehaananthan.products p on oi.order_item_product_id = p.product_id \
where p.product_price < 100.00")

top10Customers_less_than_100_DF.show()

ranks_less_than_100_DF.registerTempTable("ranks_less_than_100")
top10Customers_less_than_100_DF.registerTempTable("top10Customers_less_than_100")

sqlContext.sql("create table problem6_snehaananthan.ranks_less_than_100_output as select * from ranks_less_than_100")
sqlContext.sql("create table problem6_snehaananthan.top10Customers_less_than_100_output as select * from top10Customers_less_than_100")


########################################################
##using RDD
########################################################
pyspark --master yarn \
--num-executors 2 \
--executor-memory 1G \
--total-executor-cores 2 \
--conf spark.ui.port=12999


products = sc.textFile("/user/snehaananthan/problem6/products")
categories = sc.textFile("/user/snehaananthan/problem6/categories")

productsCategoriesJoin = products. \
map(lambda p: (int(p.split(",")[1]), p)). \
join( \
categories. \
map(lambda c: (int(c.split(",")[0]), int(c.split(",")[1]))) \
)

for i in productsCategoriesJoin.take(10): print(i)

departmentIdProductsMap = productsCategoriesJoin. \
map(lambda (k, v): 
	(int(v[1]), (int(v[0].split(",")[0]), 
				int(v[0].split(",")[1]), 
				v[0].split(",")[2], 
				float(v[0].split(",")[4]),
				v[0].split(",")[5]))
)
for i in departmentIdProductsMap.take(10): print(i)

departmentIdProductsGroup = departmentIdProductsMap.sortByKey()
for i in departmentIdProductsGroup.take(10): print(i)

departmentIdProductsGroup. \
map(lambda (k, v): 
	(sorted(list(v), key=(lambda r: r.split(",")[])))
)

