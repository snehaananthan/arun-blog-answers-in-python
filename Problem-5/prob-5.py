sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_export" \
--username retail_user \
--password itversity \
-m 3 \
--table products_snehaananthan \
--target-dir /user/snehaananthan/problem5/products-text \
--fields-terminated-by '|' \
--lines-terminated-by '\n' \
--null-string 'NOT-AVAILABLE' \
--null-non-string '-1' \
--where "product_id between 1 and 100" \
--as-textfile \
--outdir java

sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_export" \
--username retail_user \
--password itversity \
-m 2 \
--table products_snehaananthan \
--target-dir /user/snehaananthan/problem5/products-text-part1 \
--fields-terminated-by '*' \
--lines-terminated-by '\n' \
--null-string 'NA' \
--null-non-string '-1000' \
--where "product_id <= 1111" \
--as-textfile \
--outdir java

sqoop import --connect "jdbc:mysql://ms.itversity.com:3306/retail_export" \
--username retail_user \
--password itversity \
-m 5 \
--table products_snehaananthan \
--target-dir /user/snehaananthan/problem5/products-text-part2 \
--fields-terminated-by '*' \
--lines-terminated-by '\n' \
--null-string 'NA' \
--null-non-string '-1000' \
--where "product_id > 1111" \
--as-textfile \
--outdir java

sqoop merge \
--new-data /user/snehaananthan/problem5/products-text-part2 \
--onto /user/snehaananthan/problem5/products-text-part1 \
--target-dir /user/snehaananthan/problem5/products-text-both-parts \
--merge-key product_id \
--class-name products_snehaananthan \
--jar-file /tmp/sqoop-snehaananthan/compile/419ca81d9f01a6e1bc565d52c8fa5d1a/products_snehaananthan.jar

sqoop job --create first_sqoop_job \
-- import --connect "jdbc:mysql://ms.itversity.com:3306/retail_export" \
--username "retail_user" \
--password "itversity" \
-m 1 \
--table products_snehaananthan \
--target-dir /user/snehaananthan/problem5/products-incremental \
--incremental append \
--check-column product_id \
--last-value 0 \
--as-textfile \
--outdir java

sqoop job --exec first_sqoop_job

sqoop job --create sqoop_job_hive \
-- import --connect "jdbc:mysql://ms.itversity.com:3306/retail_export" \
--username retail_user \
-P \
--table products_snehaananthan \
--hive-import \
--hive-table problem5_snehaananthan.products_hive \
--incremental append \
--last-value 0 \
--check-column product_id \
--outdir java

sqoop job --exec sqoop_job_hive

sqoop job --delete sqoop_job_export

sqoop export \
--connect "jdbc:mysql://ms.itversity.com:3306/retail_export" \
--username retail_user \
-password itversity \
--table products_external_snehaananthan \
--export-dir /apps/hive/warehouse/problem5_snehaananthan.db/products_hive \
--update-key product_id \
--update-mode allowinsert \
--input-fields-terminated-by '\001' \
--columns "product_id,product_category_id,product_name,product_description,product_price,product_impage,product_grade,product_sentiment"

