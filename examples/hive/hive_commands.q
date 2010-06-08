CREATE EXTERNAL TABLE ht_table(key INT, value MAP<STRING, STRING>) STORED BY 'org.hypertable.hadoop.hive.HTStorageHandler' with SERDEPROPERTIES ("hypertable.columns.mapping" = "address:") TBLPROPERTIES ("hypertable.table.name"="HiveTest");
SELECT * FROM ht_table;
SELECT * FROM ht_table where key>2;
CREATE TABLE ht_pokes(uid INT, addr_type STRING, addr_val STRING);
LOAD DATA LOCAL INPATH './kv_ht.txt' OVERWRITE INTO TABLE ht_pokes;
SELECT * FROM ht_pokes;
INSERT OVERWRITE TABLE ht_table SELECT uid, MAP(addr_type, addr_val) FROM ht_pokes;
SELECT value FROM ht_table WHERE key <5;
