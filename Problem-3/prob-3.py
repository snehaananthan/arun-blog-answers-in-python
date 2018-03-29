sqoop import-all-tables --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
--exclude-tables order_items_nopk,orders \
--warehouse-dir /user/snehaananthan/warehouse/retail_stage.db/ \
--outdir java \
--as-avrodatafile \
--compress \
--compression-codec snappy 

sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_db" \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/snehaananthan/warehouse/retail_stage.db/orders \
--map-column-java order_date=String \
--outdir java \
--as-avrodatafile \
--compress \
--compression-codec snappy 
