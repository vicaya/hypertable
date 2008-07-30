DROP TABLE IF EXISTS hypertable;
CREATE TABLE hypertable (
apple,
banana
);
insert into hypertable VALUES ('2007-12-02 08:00:00', 'foo', 'apple:0', 'nothing'), ('2007-12-02 08:00:01', 'foo', 'apple:1', 'nothing'), ('2007-12-02 08:00:02', 'foo', 'apple:2', 'nothing');
insert into hypertable VALUES ('2007-12-02 08:00:03', 'foo', 'banana:0', 'nothing'), ('2007-12-02 08:00:04', 'foo', 'banana:1', 'nothing'), ('2007-12-02 08:00:05', 'bar', 'banana:2', 'nothing');
select * from hypertable display_timestamps;
delete "apple:1" from hypertable where row = 'foo' timestamp '2007-12-02 08:00:01';
select * from hypertable display_timestamps;
delete banana from hypertable where row = 'foo';
select * from hypertable display_timestamps;
insert into hypertable VALUES ('how', 'apple:0', 'nothing'), ('how', 'apple:1', 'nothing'), ('how', 'apple:2', 'nothing');
insert into hypertable VALUES ('now', 'banana:0', 'nothing'), ('now', 'banana:1', 'nothing'), ('now', 'banana:2', 'nothing');
insert into hypertable VALUES ('2007-12-02 08:00:00', 'lowrey', 'apple:0', 'nothing'), ('2007-12-02 08:00:00', 'season', 'apple:1', 'nothing'), ('2007-12-02 08:00:00', 'salt', 'apple:2', 'nothing');
insert into hypertable VALUES ('2028-02-17 08:00:01', 'lowrey', 'apple:0', 'nothing');
insert into hypertable VALUES ('2028-02-17 08:00:00', 'season', 'apple:1', 'nothing');
drop table if exists Pages;
create table Pages ( "refer-url", "http-code", timestamp, rowkey, ACCESS GROUP default ( "refer-url", "http-code", timestamp, rowkey ) );
insert into Pages VALUES ('2008-01-28 22:00:03',  "calendar.boston.com/abington-ma/venues/show/457680-the-cellar-tavern", 'http-code', '200' );
select "http-code" from Pages where ROW = "calendar.boston.com/abington-ma/venues/show/457680-the-cellar-tavern" display_timestamps;
delete * from Pages where ROW = "calendar.boston.com/abington-ma/venues/show/457680-the-cellar-tavern" TIMESTAMP '2008-01-28 22:00:10';
select "http-code" from Pages where ROW = "calendar.boston.com/abington-ma/venues/show/457680-the-cellar-tavern" display_timestamps;
DROP TABLE IF EXISTS hypertable;
CREATE TABLE hypertable (
a,
b
);
INSERT INTO hypertable VALUES ('2008-06-28 01:00:00', 'k1', 'a', 'a11'),('2008-06-28 01:00:00', 'k1', 'b', 'b11');
INSERT INTO hypertable VALUES ('2008-06-28 01:00:01', 'k2', 'a', 'a21'),('2008-06-28 01:00:01', 'k2', 'b', 'b21');
INSERT INTO hypertable VALUES ('2008-06-28 01:00:02', 'k2', 'b', 'b22');
INSERT INTO hypertable VALUES ('2008-06-28 01:00:03', 'k1', 'a', 'a22');
SELECT * FROM hypertable WHERE ROW = 'k1' AND TIMESTAMP < '2008-06-28 01:00:01' DISPLAY_TIMESTAMPS;
DROP TABLE IF EXISTS hypertable;
CREATE TABLE hypertable ( TestColumnFamily );
LOAD DATA INFILE ROW_KEY_COLUMN=rowkey "hypertable_test.tsv" INTO TABLE hypertable;
SELECT * FROM hypertable;
DROP TABLE IF EXISTS test;
CREATE TABLE test ( e, d );
INSERT INTO test VALUES("k1", "e", "x");
INSERT INTO test VALUES("k1", "d", "x");
DELETE d FROM test WHERE ROW = "k1";
SELECT * FROM test;
DROP TABLE IF EXISTS test;
CREATE TABLE test ( c, b );
INSERT INTO test VALUES('2008-06-30 00:00:01', "k1", "b", "x");
INSERT INTO test VALUES('2008-06-30 00:00:02', "k1", "c", "c1");
INSERT INTO test VALUES('2008-06-30 00:00:03', "k1", "c", "c2");
INSERT INTO test VALUES('2008-07-28 00:00:01', "a", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:02', "az", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:03', "b", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:04', "bz", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:05', "c", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:06', "cz", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:07', "d", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:08', "dz", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:09', "e", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:10', "fz", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:11', "g", "b", "n");
INSERT INTO test VALUES('2008-07-28 00:00:12', "gz", "b", "n");
SELECT * FROM test WHERE ('z' < ROW < 'a');
DELETE c FROM test WHERE ROW = "k1";
SELECT * FROM test WHERE TIMESTAMP < '2008-06-30 00:00:04';
SELECT * FROM test WHERE ('a' < ROW < 'b');
SELECT * FROM test WHERE ('a' < ROW <= 'b');
SELECT * FROM test WHERE ('a' <= ROW < 'b');
SELECT * FROM test WHERE ('a' <= ROW <= 'b');
SELECT * FROM test WHERE ('a' <= ROW <= 'e') and TIMESTAMP < '2008-07-28 00:00:04';
SELECT * FROM test WHERE ('a' <= ROW <= 'e') and TIMESTAMP > '2008-07-28 00:00:04';
SELECT * FROM test WHERE ('a' <= ROW <= 'e') and '2008-07-28 00:00:02' <= TIMESTAMP <= '2008-07-28 00:00:07';
SELECT * FROM test WHERE ('a' <= ROW <= 'e') and '2008-07-28 00:00:02' <= TIMESTAMP < '2008-07-28 00:00:07';
SELECT * FROM test WHERE ('a' <= ROW <= 'e') and '2008-07-28 00:00:02' < TIMESTAMP <= '2008-07-28 00:00:07';
SELECT * FROM test WHERE ('a' <= ROW <= 'e') and '2008-07-28 00:00:02' < TIMESTAMP < '2008-07-28 00:00:07';
SELECT * FROM test WHERE ROW =^ 'b';
quit
