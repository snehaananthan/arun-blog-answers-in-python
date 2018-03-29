sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
--map-column-java order_date=String \
--table orders \
--target-dir "/user/snehaananthan/problem1/orders" \
--as-avrodatafile \
--compress \
--compression-codec "org.apache.hadoop.io.compress.SnappyCodec" 

sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
--table order_items \
--target-dir "/user/snehaananthan/problem1/order_items" \
--as-avrodatafile \
--compress \
--compression-codec "org.apache.hadoop.io.compress.SnappyCodec"

sqoop list-tables --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity

hadoop fs -ls -h /user/snehaananthan/problem1/

pyspark --master yarn \
--num-executors 2 \
--executor-memory 512M \
--total-executor-cores 2 \
--packages "com.databricks:spark-avro_2.10:2.0.1" \
--conf spark.ui.port=12999 

##Using Spark SQL##

orders_DF = sqlContext.read.format("com.databricks.spark.avro").load("/user/snehaananthan/problem1/orders")
order_items_DF = sqlContext.read.format("com.databricks.spark.avro").load("/user/snehaananthan/problem1/order_items")

orders_DF.show()
order_items_DF.show()

orders_DF.registerTempTable("orders_DF")
order_items_DF.registerTempTable("order_items_DF")

totalAmountByDayPerOrderByStatus_DF = sqlContext. \
sql("select order_id, substr(order_date, 1, 10) as order_date, \
order_status, \
sum(order_item_subtotal) as subtotal_amount \
from orders_DF join order_items_DF on order_id = order_item_order_id \
group by order_id, order_date, order_status \
order by order_id")

totalAmountByDayPerOrderByStatus_DF.registerTempTable("totalAmountByDayPerOrderByStatus_DF")

results4b_DF = sqlContext. \
sql("select order_date, \
order_status, \
count(order_status) as total_orders, \
sum(subtotal_amount) as total_amount \
from totalAmountByDayPerOrderByStatus_DF \
group by order_date, order_status \
order by order_date desc, order_status asc, \
total_amount desc, total_orders asc")

results4b_DF.show()

##Usind RDDs##

orders_DF = sqlContext.read.format("com.databricks.spark.avro").load("/user/snehaananthan/problem1/orders")
order_items_DF = sqlContext.read.format("com.databricks.spark.avro").load("/user/snehaananthan/problem1/order_items")

orders = orders_DF.rdd.map(tuple)
orderItems = order_items_DF.rdd.map(tuple)

ordersMap = orders.map(lambda o: 
				(int(o[0]), (o[1][:10], o[3]))
			)
orderItemsMap = orderItems.map(lambda oi: 
					(int(oi[1]), float(oi[4]))
				)

ordersOrderItemsJoin = ordersMap.join(orderItemsMap) 
for i in ordersOrderItemsJoin.take(10): print(i)

from operator import add
orderTotalBydateByStatusPerOrderId = ordersOrderItemsJoin. \
map(lambda (k, v): 
		((k, v[0][0], v[0][1]), v[1])
	). \
reduceByKey(add)
for i in orderTotalBydateByStatusPerOrderId.take(10): print(i)

totalOrdersAmountByDatePerStatus = orderTotalBydateByStatusPerOrderId. \
map(lambda (k, v): \
		((k[1], k[2]), v)
	). \
aggregateByKey((0, 0.0), \
		lambda (total_orders, total_amount), input_value: (total_orders + 1, total_amount + input_value), \
		lambda (total_orders_1, total_amount_1), (total_orders_2, total_amount_2): \
					(total_orders_1 + total_orders_2, total_amount_1 + total_amount_2)
	)

totalOrdersAmountByDatePerStatusMap = totalOrdersAmountByDatePerStatus. \
map(lambda (k, v): \
		(k[0], (k[0], k[1], v[0], v[1]))
	)
for i in totalOrdersAmountByDatePerStatusMap.take(10): print(i)

resultsSortedByDateAndStatus = totalOrdersAmountByDatePerStatusMap. \
groupByKey(). \
sortByKey(False). \
flatMap(lambda (k, v): 
			sorted(list(v), key=(lambda l: l[1]))
		)

for i in resultsSortedByDateAndStatus.take(10): print(i)

from pyspark.sql import Row
results4c_DF = resultsSortedByDateAndStatus. \
map(lambda r: 
		Row(order_date=r[0], order_status=r[1], total_orders=int(r[2]), total_amount=float(r[3]))
	).toDF()

results4c_DF.show()

##Saving
sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
results4b_DF.write.parquet("/user/snehaananthan/problem1/result4b-gzip")
results4c_DF.write.parquet("/user/snehaananthan/problem1/result4c-gzip")

sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
results4b_DF.coalesce(6).write.parquet("/user/snehaananthan/problem1/result4b-snappy")
results4c_DF.coalesce(6).write.parquet("/user/snehaananthan/problem1/result4c-snappy")

results4b_rdd = results4b_DF.rdd.map(tuple). \
map(lambda r: r[0] + "," + r[1] + "," + str(r[2]) + "," + str(r[3]))
results4b_rdd.saveAsTextFile("/user/snehaananthan/problem1/result4b-csv")

results4c_rdd = results4c_DF.rdd.map(tuple). \
map(lambda r: r[0] + "," + r[1] + "," + str(r[2]) + "," + str(r[3]))
results4c_rdd.saveAsTextFile("/user/snehaananthan/problem1/result4c-csv")


#Export to mysql table
sqoop export --connect "jdbc:mysql://ms.itversity.com:3306/retail_export" \
--username retail_user \
--password itversity \
--table result_snehaananthan \
--export-dir "/user/snehaananthan/problem1/result4b-csv" \
--num-mappers 1