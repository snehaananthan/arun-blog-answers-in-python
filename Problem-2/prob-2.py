sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
--table products \
--target-dir "/user/snehaananthan/products" \
--fields-terminated-by '|' \
--m 4

hadoop fs -ls /user/snehaananthan/
hadoop fs -mkdir /user/snehaananthan/problem2
hadoop fs -mv /user/snehaananthan/products /user/snehaananthan/problem2/ 
hadoop fs -chmod 765 /user/snehaananthan/problem2/products/
hadoop fs -ls -h /user/snehaananthan/problem2/products/
hadoop fs -chown snehaananthan /user/snehaananthan/*

mysql --host=ms.itversity.com --port=3306 --user=retail_user --password=itversity

pyspark --master yarn \
--num-executors 2 \
--executor-memory 512M \
--total-executor-cores 2 \
--packages "com.databricks:spark-avro_2.10:2.0.1" \
--conf spark.ui.port=12999

##using RDDs

products = sc.textFile("/user/snehaananthan/problem2/products")

productsFiltered = products.filter(lambda p: float(p.split("|")[4]) < 100.00)

productsMap = productsFiltered. \
map(lambda p: (int(p.split("|")[1]), float(p.split("|")[4])))


results = productsMap. \
aggregateByKey(
	(0.0, 0, 0.0, 101.00), 
	lambda (highest, count, sum, lowest), input_price: 
		(input_price if input_price > highest else highest, 
		count + 1, 
		sum + input_price, 
		input_price if input_price < lowest else lowest), 
	lambda (highest1, count1, sum1, lowest1), (highest2, count2, sum2, lowest2): 
		(highest1 if highest1 > highest2 else highest2, 
		count1 + count2, 
		sum1 + sum2 / count1 + count2, 
		lowest1 if lowest1 < lowest2 else lowest2
		)
).sortByKey()


from pyspark.sql import Row
results_DF = results.map(lambda r: Row(product_category_id=r[0], 
							highest_price=r[1][0], 
							total_products=r[1][1], 
							average_price=r[1][2], 
							lowest_price=r[1][3])
		).toDF()


results_DF.write. \
format("com.databricks.spark.avro"). \
option("codec", "org.apache.hadoop.io.compress.SnappyCodec"). \
save("/user/snehaananthan/problem2/solution/avro/result-rdd")


##using Spark-SQL

products = sc.textFile("/user/snehaananthan/problem2/products")

from pyspark.sql import Row
products_DF = products. \
map(lambda p: 
	Row(product_category_id=int(p.split("|")[1]),
		product_price=float(p.split("|")[4])
	)
).toDF()

products_DF.registerTempTable("products_DF")

results_DF = sqlContext.sql("select product_category_id, max(product_price) as highest_price, \
count(product_price) as total_products, \
avg(product_price) as average_price, \
min(product_price) as minimum_price \
from products_DF \
group by product_category_id \
order by product_category_id")

results_DF.show()

results_DF. \
coalesce(4). \
write. \
format("com.databricks.spark.avro"). \
option("codec", "org.apache.hadoop.io.compress.SnappyCodec"). \
save("/user/snehaananthan/problem2/solution/avro/result-sql")
