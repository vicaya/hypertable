USE '/test';
DROP TABLE IF EXISTS hypertable;
CREATE TABLE hypertable(TestColumnFamily);
LOAD DATA INFILE ROW_KEY_COLUMN=rowkey "-" INTO TABLE hypertable;
# rowkey	TestColumnFamily
key1	tcf1
key2	tcf2

