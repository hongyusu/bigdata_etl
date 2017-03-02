# ingestion from hive to mysql

sqoop export \
    --connect jdbc:mysql://localhost/test \
    --username root \
    --password pwd \
    --table ratings \
    --export-dir /user/hive/warehouse/test.db/ratings \
    -m 1

exit

# ingestion from mysql to hive

sqoop import \
   --connect jdbc:mysql://localhost/test \
   --username root \
   --P \
   --table testtable \
   --hive-import \
   --hive-database test \
   --create-hive-table \
   --hive-table testtable \
   -m 1 



# ingestion from hive to hdfs : table

sqoop import \
    --connect jdbc:mysql://localhost/test \
    --username root \
    --P \
    --table testtable \
    --target-dir ./testtable_table \
    -m 1

# ingestion from mysql to hdfs : query

sqoop import \
    --connect jdbc:mysql://localhost/test \
    --username root \
    --P \
    --query "select * from testtable where \$CONDITIONS" \
    --target-dir ./testtable_query \
    -m 1

# ingestion from mysql to hdfs : whole table

sqoop import \
    --connect jdbc:mysql://localhost/test \
    --username root \
    --P \
    --table testtable \
    --target-dir ./testtable_table \
    -m 1



