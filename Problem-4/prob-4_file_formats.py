sqoop import --connect "jdbc:mysql://ms.itversity.com/retail_db" \
--username retail_user \
--password itversity \
--table orders \
--map-column-java order_date=String \
--target-dir /user/snehaananthan/problem5/text \
--as-textfile \
--outdir java \
--fields-terminated-by '\t'

sqoop import --connect "jdbc:mysql://ms.itversity.com/retail_db" \
--username retail_user \
--password itversity \
--table orders \
--map-column-java order_date=String \
--target-dir /user/snehaananthan/problem5/avro \
--as-avrodatafile

sqoop import --connect "jdbc:mysql://ms.itversity.com/retail_db" \
--username retail_user \
--password itversity \
--table orders \
--map-column-java order_date=String \
--target-dir /user/snehaananthan/problem5/parquet \
--as-parquetfile


pyspark --master yarn \
--num-executors 2 \
--executor-memory 512M \
--total-executor-cores 2 \
--packages "com.databricks:spark-avro_2.10:2.0.1" \
--conf spark.ui.port=12999

##read avro files
avro_DF = sqlContext.read.format("com.databricks.spark.avro"). \
load("/user/snehaananthan/problem5/avro")

##write as parquet file with snappy compression
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
avro_DF.write.parquet("/user/snehaananthan/problem5/parquet-snappy-compress")

##save as text file with gzip compression
text_RDD = avro_DF.rdd.map(tuple). \
map(lambda r: str(r[0]) + "\t" + r[1] + "\t" + str(r[2]) + "\t" + r[3])

text_RDD.saveAsTextFile("/user/snehaananthan/problem5/text-gzip-compress", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

##save as sequence file
text_RDD_seq = avro_DF.rdd.map(tuple). \
map(lambda r: (str(r[0]), r[1] + "\t" + str(r[2]) + "\t" + r[3]))

text_RDD_seq.saveAsSequenceFile("/user/snehaananthan/problem5/sequence")

##save as text file with snappy compression
text_RDD.saveAsTextFile("/user/snehaananthan/problem5/text-snappy-compress", compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")

##read parquet files
parquet_DF = sqlContext.read.parquet("/user/snehaananthan/problem5/parquet-snappy-compress")

##write as parquet file without compression
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")
parquet_DF.write.parquet("/user/snehaananthan/problem5/parquet-no-compress")

##write as avro file with snappy compression
parquet_DF.write.format("com.databricks.spark.avro"). \
option("codec", "org.apache.hadoop.io.compress.SnappyCodec"). \
save("/user/snehaananthan/problem5/avro-snappy")

##read avro files
avro_snappy_DF = sqlContext.read.format("com.databricks.spark.avro"). \
load("/user/snehaananthan/problem5/avro-snappy")

##write as json files without compression
avro_snappy_DF.write.json("/user/snehaananthan/problem5/json-no-compress")
## or ##
avro_snappy_DF.toJSON(). \
saveAsTextFile("/user/snehaananthan/problem5/json-no-compress")

##write as json files with gzip compression
avro_snappy_DF.toJSON(). \
saveAsTextFile("/user/snehaananthan/problem5/json-gzip", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

##read json files
json_gzip_DF = sqlContext.read.json("/user/snehaananthan/problem5/json-gzip")
json_gzip_DF.show()
##also##
json_gzip_RDD = sc.textFile("/user/snehaananthan/problem5/json-gzip")
for i in json_gzip_RDD.take(10): print(i)

##save as csv formatted text file
json_text_RDD = json_gzip_DF.rdd. \
map(tuple). \
map(lambda r: 
	str(r[0]) + "," + r[1] + "," + str(r[2]) + "," + r[3]
)
json_text_RDD.saveAsTextFile("/user/snehaananthan/problem5/csv-no-compress")

##save as csv formatted text file with gzip compression
json_text_RDD.saveAsTextFile("/user/snehaananthan/problem5/csv-gzip", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

##read sequence files
seq_RDD = sc.sequenceFile("/user/snehaananthan/problem5/sequence")

##write as orc files
from pyspark.sql import Row
seq_DF = seq_RDD. \
map(lambda r: 
	Row(order_id=int(r[0]), order_date=r[1].split("\t")[0], order_customer_id=int(r[1].split("\t")[1]), order_status=r[1].split("\t")[2])
).toDF()
seq_DF.write.orc("/user/snehaananthan/problem5/orc")