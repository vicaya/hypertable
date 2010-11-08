CREATE EXTERNAL TABLE ht_borders(maritime_exclusive_zone INT, maritime_territorial_sea INT, country STRING, neighbours MAP<STRING, STRING>) STORED BY 'org.hypertable.hadoop.hive.HTStorageHandler' with SERDEPROPERTIES ("hypertable.columns.mapping" = "maritime:exclusive_zone,maritime:territorial_sea,:key,neighbours:") TBLPROPERTIES ("hypertable.table.name"="Borders","hypertable.table.namespace"="/");
SELECT * FROM ht_borders;
SELECT country, neighbours from ht_borders;
