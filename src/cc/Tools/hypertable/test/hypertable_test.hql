CREATE NAMESPACE "/test";
USE "/test";
CREATE TABLE foo('test');
CREATE NAMESPACE "subtest";
GET LISTING;
SHOW TABLES;
USE "subtest";
CREATE TABLE foo('subtest');
USE "/test/subtest";
USE "/test/subtest/subsubtest";
SHOW CREATE TABLE foo;
DROP TABLE foo;
DROP NAMESPACE "/test";
USE "/test";
DROP NAMESPACE "subtest";
RENAME TABLE foo TO foo_renamed;
GET LISTING;
DROP TABLE foo;
DROP TABLE foo_renamed;
DROP NAMESPACE "/test";
CREATE TABLE foo('test');
CREATE NAMESPACE "/test";
USE "/test";
DROP TABLE IF EXISTS hypertable;
EXISTS TABLE hypertable;
CREATE TABLE hypertable (
apple,
banana
);
EXISTS TABLE hypertable;
insert into hypertable VALUES ('2007-12-02 08:00:00', 'foo', 'apple:0', 'nothing'), ('2007-12-02 08:00:01', 'foo', 'apple:1', 'nothing'), ('2007-12-02 08:00:02', 'foo', 'apple:2', 'nothing');
insert into hypertable VALUES ('2007-12-02 08:00:03', 'foo', 'banana:0', 'nothing'), ('2007-12-02 08:00:04', 'foo', 'banana:1', 'nothing'), ('2007-12-02 08:00:05', 'bar', 'banana:2', 'nothing');
select * from hypertable display_timestamps;
delete "apple:1" from hypertable where row = 'foo' timestamp '2007-12-02 08:00:01';
select * from hypertable display_timestamps;
delete banana from hypertable where row = 'foo';
select * from hypertable display_timestamps;
select * from hypertable where cell="foo","banana:0" display_timestamps;
insert into hypertable VALUES ('how', 'apple:0', 'nothing'), ('how', 'apple:1', 'nothing'), ('how', 'apple:2', 'nothing');
insert into hypertable VALUES ('now', 'banana:0', 'nothing'), ('now', 'banana:1', 'nothing'), ('now', 'banana:2', 'nothing');
insert into hypertable VALUES ('2007-12-02 08:00:00', 'lowrey', 'apple:0', 'nothing'), ('2007-12-02 08:00:00', 'season', 'apple:1', 'nothing'), ('2007-12-02 08:00:00', 'salt', 'apple:2', 'nothing');
insert into hypertable VALUES ('2028-02-17 08:00:01', 'lowrey', 'apple:0', 'nothing');
insert into hypertable VALUES ('2028-02-17 08:00:00', 'season', 'apple:1', 'nothing');
drop table if exists Pages;
create table Pages ("refer_url", "http_code", timestamp, rowkey, ACCESS GROUP default ("refer_url", "http_code"), ACCESS GROUP misc (timestamp, rowkey));
insert into Pages VALUES ('2008-01-28 22:00:03',  "calendar.boston.com/abington-ma/venues/show/457680-the-cellar-tavern", 'http_code', '200');
select http_code from Pages where ROW = "calendar.boston.com/abington-ma/venues/show/457680-the-cellar-tavern" display_timestamps;
delete * from Pages where ROW = "calendar.boston.com/abington-ma/venues/show/457680-the-cellar-tavern" TIMESTAMP '2008-01-28 22:00:10';
select http_code from Pages where ROW = "calendar.boston.com/abington-ma/venues/show/457680-the-cellar-tavern" display_timestamps;
select * from Pages where CELL = "calendar.boston.com/abington-ma/venues/show/457680-the-cellar-tavern","http_code" display_timestamps;
DROP TABLE IF EXISTS Pages_clone;
CREATE TABLE Pages_clone LIKE Pages;
SHOW CREATE TABLE Pages_clone;
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
SELECT * FROM hypertable CELL_LIMIT=1 LIMIT=1;
SELECT * FROM hypertable CELL_LIMIT=1 REVS=2;
SELECT * FROM hypertable CELL_LIMIT=2 REVS=2;
SELECT * FROM hypertable CELL_LIMIT=1;
SELECT * FROM hypertable LIMIT=1 REVS=2;
DROP TABLE IF EXISTS hypertable;
CREATE TABLE hypertable ( TestColumnFamily );
LOAD DATA INFILE ROW_KEY_COLUMN=rowkey "hypertable_test.tsv" INTO TABLE hypertable;
LOAD DATA INFILE ROW_KEY_COLUMN=rowkey IGNORE_UNKNOWN_CFS "hypertable_unknown_cf.tsv" INTO TABLE hypertable;
SELECT * FROM hypertable;
LOAD DATA INFILE ROW_KEY_COLUMN=rowkey "hypertable_unknown_cf.tsv" INTO TABLE hypertable;
DROP TABLE IF EXISTS hypertable;
CREATE TABLE hypertable ( TestColumnFamily );
LOAD DATA INFILE ROW_KEY_COLUMN=rowkey "hypertable_ts.tsv" INTO TABLE hypertable;
SELECT * from hypertable WHERE TIMESTAMP > '2009-02-12 02:12:12.100000000' DISPLAY_TIMESTAMPS;
SELECT * from hypertable WHERE TIMESTAMP < '2009-02-12 02:12:13:4' DISPLAY_TIMESTAMPS;
SELECT * from hypertable WHERE TIMESTAMP <= '2009-02-12 02:12:15' DISPLAY_TIMESTAMPS;
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
SELECT * FROM test WHERE (ROW = 'a' or ROW = 'c' or ROW = 'g');
SELECT * FROM test WHERE ('a' < ROW <= 'c' or ROW = 'g' or ROW = 'c');
SELECT * FROM test WHERE (ROW < 'c' or ROW > 'd');
SELECT * FROM test WHERE (ROW < 'b' or ROW =^ 'b');
SELECT * FROM test WHERE (ROW =^'a' or 'd' < ROW < 'b' or ROW =^ 'b');
DROP TABLE IF EXISTS test;
CREATE TABLE test ( tag );
INSERT INTO test VALUES("how", "tag:A", "n");
INSERT INTO test VALUES("now", "tag:a", "n");
INSERT INTO test VALUES("brown", "tag:aa", "n");
INSERT INTO test VALUES("cow", "tag:aal", "n");
INSERT INTO test VALUES("how", "tag:aalii", "n");
INSERT INTO test VALUES("now", "tag:aam", "n");
INSERT INTO test VALUES("brown", "tag:Aani", "n");
INSERT INTO test VALUES("cow", "tag:aardvark", "n");
INSERT INTO test VALUES("how", "tag:aardwolf", "n");
INSERT INTO test VALUES("now", "tag:Aaron", "n");
INSERT INTO test VALUES("brown", "tag:Aaronic", "n");
INSERT INTO test VALUES("cow", "tag:Aaronical", "n");
INSERT INTO test VALUES("how", "tag:Aaronite", "n");
INSERT INTO test VALUES("now", "tag:Aaronitic", "n");
INSERT INTO test VALUES("brown", "tag:Aaru", "n");
INSERT INTO test VALUES("cow", "tag:Ab", "n");
INSERT INTO test VALUES("old", "tag:aba", "n");
INSERT INTO test VALUES("macdonald", "tag:Ababdeh", "n");
INSERT INTO test VALUES("had", "tag:Ababua", "n");
INSERT INTO test VALUES("a", "tag:abac", "n");
INSERT INTO test VALUES("farm", "tag:abaca", "n");
INSERT INTO test VALUES("old", "tag:abacate", "n");
INSERT INTO test VALUES("macdonald", "tag:abacay", "n");
INSERT INTO test VALUES("had", "tag:abacinate", "n");
INSERT INTO test VALUES("a", "tag:abacination", "n");
INSERT INTO test VALUES("farm", "tag:abaciscus", "n");
INSERT INTO test VALUES("old", "tag:abacist", "n");
INSERT INTO test VALUES("macdonald", "tag:aback", "n");
INSERT INTO test VALUES("had", "tag:abactinal", "n");
INSERT INTO test VALUES("a", "tag:abactinally", "n");
INSERT INTO test VALUES("farm", "tag:abaction", "n");
INSERT INTO test VALUES("old", "tag:abactor", "n");
INSERT INTO test VALUES("macdonald", "tag:abaculus", "n");
INSERT INTO test VALUES("had", "tag:abacus", "n");
INSERT INTO test VALUES("a", "tag:Abadite", "n");
INSERT INTO test VALUES("farm", "tag:abaff", "n");
INSERT INTO test VALUES("kaui", "tag:abaft", "n");
INSERT INTO test VALUES("maui", "tag:abaisance", "n");
INSERT INTO test VALUES("oahu", "tag:abaiser", "n");
INSERT INTO test VALUES("kaui", "tag:abaissed", "n");
INSERT INTO test VALUES("maui", "tag:abalienate", "n");
INSERT INTO test VALUES("oahu", "tag:abalienation", "n");
INSERT INTO test VALUES("bar", "tag:abalone", "n");
INSERT INTO test VALUES("bar", "tag:Abama", "n");
INSERT INTO test VALUES("bar", "tag:abampere", "n");
INSERT INTO test VALUES("bar", "tag:abandon", "n");
INSERT INTO test VALUES("bar", "tag:abandonable", "n");
INSERT INTO test VALUES("bar", "tag:abandoned", "n");
INSERT INTO test VALUES("bar", "tag:abandonedly", "n");
INSERT INTO test VALUES("bar", "tag:acutonodose", "n");
INSERT INTO test VALUES("foo", "tag:acutorsion", "n");
INSERT INTO test VALUES("foo", "tag:acyanoblepsia", "n");
INSERT INTO test VALUES("foo", "tag:acyanopsia", "n");
INSERT INTO test VALUES("bar", "tag:acyclic", "n");
INSERT INTO test VALUES("foo", "tag:acyesis", "n");
INSERT INTO test VALUES("foo", "tag:acyetic", "n");
INSERT INTO test VALUES("foo", "tag:acystia", "n");
INSERT INTO test VALUES("bar", "tag:ad", "n");
INSERT INTO test VALUES("foo", "tag:Ada", "n");
INSERT INTO test VALUES("foo", "tag:adactyl", "n");
INSERT INTO test VALUES("foo", "tag:adactylia", "n");
INSERT INTO test VALUES("foo", "tag:adactylism", "n");
INSERT INTO test VALUES("foo", "tag:adactylous", "n");
INSERT INTO test VALUES("foo", "tag:adage", "n");
INSERT INTO test VALUES("bar", "tag:adagial", "n");
INSERT INTO test VALUES("foo", "tag:adagietto", "n");
INSERT INTO test VALUES("foo", "tag:adamantoblast", "n");
INSERT INTO test VALUES("bar", "tag:adamantoblastoma", "n");
INSERT INTO test VALUES("foo", "tag:adamantoid", "n");
SELECT * from test WHERE "farm","tag:abaca" < CELL < "had","tag:abacinate";
SELECT * from test WHERE "farm","tag:abaca" <= CELL < "had","tag:abacinate";
SELECT * from test WHERE "farm","tag:abaca" < CELL <= "had","tag:abacinate";
SELECT * from test WHERE "farm","tag:abaca" <= CELL <= "had","tag:abacinate";
SELECT * from test WHERE CELL = "foo","tag:adactylism";
SELECT * from test WHERE CELL =^ "foo","tag:ac";
SELECT * from test WHERE CELL =^ "foo","tag:a";
SELECT * from test WHERE CELL > "old","tag:abacate";
SELECT * from test WHERE CELL >= "old","tag:abacate";
SELECT * from test WHERE "old","tag:foo" < CELL >= "old","tag:abacate";
SELECT * FROM test WHERE ( CELL = "maui","tag:abaisance" OR CELL = "foo","tag:adage" OR CELL = "cow","tag:Ab" OR CELL =^ "foo","tag:acya");
DROP TABLE IF EXISTS test;
CREATE TABLE test ( name, address, tag, phone );
INSERT INTO test VALUES("foo", "name", "Joe");
INSERT INTO test VALUES("foo", "address", "1234 Main Street");
INSERT INTO test VALUES("foo", "tag", "test");
INSERT INTO test VALUES("foo", "tag:height", "5'9");
INSERT INTO test VALUES("foo", "tag:weight", "150lb");
INSERT INTO test VALUES("foo", "phone", "2455542");
SELECT * from test WHERE "foo","tag" <= CELL < "foo","phone";

#
# Issue 154
#
CREATE TABLE bug ( F MAX_VERSIONS=1 );
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V1');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V2');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V3');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V4');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V5');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V6');
SELECT * FROM bug;
DROP TABLE bug;
CREATE TABLE bug ( F MAX_VERSIONS=2 );
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V1');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V2');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V3');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V4');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V5');
SELECT * FROM bug;
DELETE 'F:Q' FROM bug WHERE ROW = 'R';
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V6');
INSERT INTO bug VALUES ('R','F:Q','V7');
SELECT * FROM bug;
INSERT INTO bug VALUES ('R','F:Q','V8');
SELECT * FROM bug;
DROP TABLE bug;
DROP TABLE IF EXISTS column_family_ttl;
CREATE TABLE test_column_family_ttl (
apple TTL = 3 seconds,
banana TTL = 6 seconds,
persistent 
);
insert into test_column_family_ttl VALUES ('foo', 'apple:0', 'nothing'), ('foo', 'apple:1', 'nothing'), ('bar', 'apple:2', 'nothing');
insert into test_column_family_ttl VALUES ('foo', 'banana:0', 'nothing'), ('bar', 'banana:1', 'nothing'), ('bar', 'banana:2', 'nothing');
insert into test_column_family_ttl VALUES ('foo', 'persistent:0', 'nothing'), ('foo', 'persistent:1', 'nothing'), ('bar', 'persistent:2', 'nothing');
select * from test_column_family_ttl;
pause 4;
select * from test_column_family_ttl;
pause 4;
select * from test_column_family_ttl;
DROP TABLE test_column_family_ttl;
DROP TABLE IF EXISTS Pages;
CREATE TABLE Pages (
 'refer-url',
 'http-code',
 date,
 ACCESS GROUP default ( 'refer-url', 'http-code' )
);
insert into Pages values("2008-11-11 12:00:00.000000","www.google.com", "refer-url", "www.yahoo.com");
insert into Pages values("2008-11-11 12:00:00.000000","www.google.com", "http-code", "200");
insert into Pages values("2008-11-11 12:00:00.000000","www.google.com", "date", "2008/11/11");
insert into Pages values("2008-11-12 12:00:00.000000","www.google.com", "refer-url", "www.yahoo.com");
insert into Pages values("2008-11-12 12:00:00.000000","www.google.com", "http-code", "404");
insert into Pages values("2008-11-12 12:00:00.000000","www.google.com", "date", "2008/11/12");
select * from Pages display_timestamps;
delete 'http-code' from Pages where row="www.google.com";
select * from Pages display_timestamps;

#
#ALTER TABLE
#
DROP table if exists Fruits;
CREATE TABLE Fruits (
 'refer-url',
 'http-code',
 ACCESS GROUP ag1 ( 'refer-url'), 
 ACCESS GROUP ag2 ( 'http-code' )
);
insert into Fruits values("www.google.com", "refer-url", "www.yahoo.com");
insert into Fruits values("www.google.com", "http-code", "200");
ALTER TABLE Fruits ADD(
  'Red', 
  ACCESS GROUP ag3 ('Red')
) DROP ('http-code')
;
insert into Fruits values("www.google.com", "Red", "Apple");
insert into Fruits values("www.yahoo.com", "Red", "Grapefruit");
select * from Fruits;
select * from Fruits where row="www.google.com";

ALTER TABLE Fruits ADD ('Orange') DROP('Red') 
    ADD('Green', 'Yellow', ACCESS GROUP ag4 ('Green', 'Yellow'));
insert into Fruits values("www.google.com", "Green", "Lime");
insert into Fruits values("www.yahoo.com", "Orange", "Clementine");
insert into Fruits values("www.yahoo.com", "Yellow", "Banana");

select * from Fruits;
ALTER TABLE Fruits ADD(
'Red'
);

ALTER TABLE Fruits RENAME COLUMN FAMILY ('Orange', 'Pink') RENAME COLUMN FAMILY ('refer-url', 'referral');
insert into Fruits values('www.cnn.com', 'Orange', 'Kumquat');
insert into Fruits values('www.cnn.com', 'Pink', 'Grapefruit');
select * from Fruits;
ALTER TABLE Fruits RENAME COLUMN FAMILY ('Orange', 'Blue');
ALTER TABLE Fruits RENAME COLUMN FAMILY ('Pink', 'Green');

alter table Fruits add ('7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176', '177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196', '197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236', '237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255') ;

alter table Fruits drop ('7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176', '177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189', '190', '191', '192', '193', '194', '195', '196', '197', '198', '199', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236', '237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249', '250', '251', '252', '253', '254', '255') ;
alter table Fruits add('256');
DESCRIBE TABLE WITH IDS Fruits;

DROP table if exists render_bug;
create table render_bug (
       foo,
       bar,
       ACCESS GROUP foo_group ( foo ),
       ACCESS GROUP bar_group ( bar )
);

show create table render_bug;
#
# SELECT INTO GZ FILE
#
DROP table if exists Fruits;
CREATE TABLE Fruits (
 'refer-url',
 'http-code',
 ACCESS GROUP ag1 ( 'refer-url'), 
 ACCESS GROUP ag2 ( 'http-code' )
);
insert into Fruits values("www.google.com", "refer-url", "www.yahoo.com");
insert into Fruits values("www.google.com", "http-code", "200");
SELECT * FROM Fruits INTO FILE 'hypertable_select_gz_test.output.gz';

DROP table if exists Fruits;
CREATE TABLE Fruits (
  apple TTL=2 DAYS,
  banana MAX_VERSIONS=2,
  carrot,
  ACCESS GROUP foo BLOCKSIZE=20000 ( banana )
) IN_MEMORY BLOCKSIZE=10000 TTL=1 WEEK MAX_VERSIONS=3;
SHOW CREATE TABLE Fruits;

DROP TABLE IF EXISTS test;
CREATE TABLE test ( foo, bar );
LOAD DATA INFILE DUP_KEY_COLS "foobar.tsv" INTO TABLE test;
LOAD DATA INFILE SINGLE_CELL_FORMAT "single_cell_test.tsv" INTO TABLE test;
SELECT * from test;
create table "foo-bar" ( a ) ;
insert into "foo-bar" values ("1965-02-23 07:00:00", "hello", "a", "wow");
select * from "foo-bar" display_timestamps;
drop table "foo-bar";
#issue 175
DROP TABLE IF EXISTS test;
CREATE TABLE test ('col');
INSERT INTO test VALUES('1234','col','300');
SELECT * FROM test;
DROP TABLE IF EXISTS test;
#replication factor
CREATE TABLE reptest1 ( a, b ) REPLICATION=2;
CREATE TABLE reptest2 (
       a,
       b,
       c,
       ACCESS GROUP ag1 REPLICATION=2 ( a ),
       ACCESS GROUP ag2 REPLICATION=7 ( b )
       ) REPLICATION=5;
DESCRIBE TABLE reptest1;
SHOW CREATE TABLE reptest1;
DESCRIBE TABLE reptest2;
SHOW CREATE TABLE reptest2;
DROP TABLE IF EXISTS hypertable;
CREATE TABLE hypertable ( col1, col2 );
INSERT INTO hypertable VALUES ('2010-04-12 10:19:32', 'Russell', 'col1', 'first');
INSERT INTO hypertable VALUES ('2010-04-12 10:19:32', 'Russell', 'col1', 'second');
INSERT INTO hypertable VALUES ('2010-04-12 10:19:32', 'Russell', 'col2', 'third');
INSERT INTO hypertable VALUES ('2010-04-12 10:19:32', 'Stover', 'col1:foo', 'first');
INSERT INTO hypertable VALUES ('2010-04-12 10:19:32', 'Stover', 'col1:foo', 'second');
INSERT INTO hypertable VALUES ('2010-04-12 10:19:32', 'Stover', 'col2:foo', 'third');
SELECT * FROM hypertable DISPLAY_TIMESTAMPS;
DROP TABLE IF EXISTS hypertable;
CREATE TABLE hypertable ( 'media:image' );
DROP TABLE IF EXISTS CounterTest;
CREATE TABLE CounterTest(cf1 COUNTER, 
                         cf2, 
                         cf3, 
                         cf4 COUNTER, 
                         ACCESS GROUP ag1(cf1, cf2), 
                         ACCESS GROUP ag2(cf3, cf4));
SHOW CREATE TABLE CounterTest;
DESCRIBE TABLE CounterTest;
INSERT INTO CounterTest VALUES ('row0', 'cf1:cq1', '0');
INSERT INTO CounterTest VALUES ('row0', 'cf1:cq1', '3');
SELECT * from CounterTest;
DELETE * FROM CounterTest where ROW = "row0";
DELETE 'cf1' FROM CounterTest where ROW = "row1";
INSERT INTO CounterTest VALUES ('row0', 'cf1:cq1', '2');
INSERT INTO CounterTest VALUES ('row0', 'cf1:cq1', '29');
INSERT INTO CounterTest VALUES ('row0', 'cf1:cq2', '5');
INSERT INTO CounterTest VALUES ('row0', 'cf1:cq2', '2');
INSERT INTO CounterTest VALUES ('row0', 'cf3:cq1', 'Hello');
INSERT INTO CounterTest VALUES ('row0', 'cf3:cq1', 'World');
INSERT INTO CounterTest VALUES ('row0', 'cf4:cq1', '6');
INSERT INTO CounterTest VALUES ('row0', 'cf4:cq1', '5');
INSERT INTO CounterTest VALUES ('row1', 'cf1', '13');
INSERT INTO CounterTest VALUES ('row1', 'cf2:cq1', 'Foo1');
INSERT INTO CounterTest VALUES ('row1', 'cf1', '3');
INSERT INTO CounterTest VALUES ('row1', 'cf1', '1');
DELETE 'cf1:cq1' FROM CounterTest WHERE ROW = 'row0';
DELETE 'cf4' FROM CounterTest WHERE ROW = 'row0';
INSERT INTO CounterTest VALUES ('row0', 'cf4:cq1', '2');
INSERT INTO CounterTest VALUES ('row0', 'cf4:cq1', '4');
SELECT * from CounterTest WHERE "row0","cf1:cq1" <= CELL <= "row1","cf1:cq1" RETURN_DELETES; 
INSERT INTO CounterTest VALUES ('row2', 'cf2', 'Foo2');
INSERT INTO CounterTest VALUES ('row1', 'cf1:cq1', '6');
INSERT INTO CounterTest VALUES ('row1', 'cf1:cq1', '7');
SELECT * from CounterTest WHERE ROW >= 'row1';
SELECT * from CounterTest WHERE CELL > 'row0','cf1:cq2' LIMIT=2 CELL_LIMIT=1 MAX_VERSIONS=2;
INSERT INTO CounterTest VALUES ('row1', 'cf1:cq1', '=3');
INSERT INTO CounterTest VALUES ('row1', 'cf1:cq1', '+2');
INSERT INTO CounterTest VALUES ('row1', 'cf1:cq1', '7');
INSERT INTO CounterTest VALUES ('row1', 'cf1:cq1', '-2');
SELECT * from CounterTest WHERE ROW = 'row1';
CREATE TABLE CounterTest2(cf1, cf2, cf3,
                         ACCESS GROUP ag1 COUNTER (cf1, cf2));
INSERT INTO CounterTest2 VALUES ('row1', 'cf1:cq1', '=3');
INSERT INTO CounterTest2 VALUES ('row1', 'cf1:cq1', '+2');
INSERT INTO CounterTest2 VALUES ('row1', 'cf1:cq1', '7');
INSERT INTO CounterTest2 VALUES ('row1', 'cf1:cq1', '-2');
SELECT * from CounterTest2 WHERE ROW = 'row1';
SHOW CREATE TABLE CounterTest2;
DROP NAMESPACE IF EXISTS badns;

# test regexp filtering
DROP TABLE IF EXISTS RegexpTest;
CREATE TABLE RegexpTest('col1', 'col2');
INSERT INTO RegexpTest VALUES('suitability', 'col1', 'centrist'); 
INSERT INTO RegexpTest VALUES('http://yahoo.com', 'col2:bird', 'pheasant');
INSERT INTO RegexpTest VALUES('http://yahoo.com', 'col2:mailto', 'bodhi2sattva17');
INSERT INTO RegexpTest VALUES('http://www.dmv.ca.gov', 'col1:fish', 'salmon tuna');
INSERT INTO RegexpTest VALUES('moss berry', 'col1:word', 'panini'); 
INSERT INTO RegexpTest VALUES('martingale_123', 'col2:ergodic1', 'Consignificative65Era');
INSERT INTO RegexpTest VALUES('moss berry', 'col1:w1234', 'giants'); 
INSERT INTO RegexpTest VALUES('orange marmalade', 'col1:w1234', 'clone'); 
SELECT * from RegexpTest WHERE ROW REGEXP "^[sm]\D+$"; 
SELECT col2:bird from RegexpTest WHERE ROW REGEXP "http://.*"; 
SELECT col2:"bird" from RegexpTest WHERE ROW REGEXP "http://.*"; 
SELECT col2:"bird",col2:/mail/ from RegexpTest WHERE ROW REGEXP "http://.*"; 
SELECT col1:/^w[^a-zA-Z]*$/ from RegexpTest WHERE ROW REGEXP "m.*\s\S";
SELECT CELLS col1:/^w[^a-zA-Z]*$/ from RegexpTest WHERE ROW REGEXP "^\D+" AND VALUE REGEXP "l.*e";
SELECT CELLS col1:/^w/, col2:/^[em].*/ from RegexpTest WHERE VALUE REGEXP "i.*a";

# test empty qualifier filtering
INSERT INTO RegexpTest VALUES('http://yahoo.com', 'col2', 'swiss');
SELECT col1, col2 from RegexpTest;
SELECT col1:"", col2:"" from RegexpTest;
SELECT CELLS col1:"", col2:"" from RegexpTest;
SELECT col1:"" from RegexpTest WHERE ROW = 'suitability';
SELECT CELLS col1:"" from RegexpTest WHERE ROW = 'suitability';
SELECT col2:"" from RegexpTest WHERE ROW = 'http://yahoo.com';
SELECT CELLS col2:"" from RegexpTest WHERE ROW = 'http://yahoo.com';

# test scan and filter rows
SELECT col1, col2 from RegexpTest SCAN_AND_FILTER_ROWS;
SELECT col1:"", col2:"" from RegexpTest SCAN_AND_FILTER_ROWS;
SELECT CELLS col1:"", col2:"" from RegexpTest SCAN_AND_FILTER_ROWS;
SELECT col1:"" from RegexpTest WHERE ROW = 'suitability' SCAN_AND_FILTER_ROWS;
SELECT * from RegexpTest WHERE (ROW = 'suitability' OR ROW = 'moss berry') SCAN_AND_FILTER_ROWS;
SELECT * from RegexpTest WHERE (ROW = 'suitability' OR ROW = 'moss berry' OR ROW = 'orange marmalade') SCAN_AND_FILTER_ROWS;
SELECT * from RegexpTest WHERE (ROW = 'suitability' OR ROW = 'moss berry' OR ROW = 'orange marmalade' OR ROW = 'http://yahoo.com') SCAN_AND_FILTER_ROWS;
SELECT * from RegexpTest WHERE (ROW = 'suitability' OR ROW = 'moss berry' OR ROW = 'orange marmalade' OR ROW = 'http://yahoo.com' OR ROW = 'http://www.dmv.ca.gov' OR ROW = 'martingale_123') SCAN_AND_FILTER_ROWS;
SELECT * from RegexpTest WHERE ROW REGEXP "http://.*" AND (ROW = 'http://www.dmv.ca.gov' OR ROW = 'moss berry' OR ROW = 'orange marmalade' OR ROW = 'http://yahoo.com') SCAN_AND_FILTER_ROWS;
SELECT CELLS * from RegexpTest WHERE (ROW = 'suitability' OR ROW = 'moss berry' OR ROW = 'orange marmalade') AND VALUE REGEXP "^c.*" SCAN_AND_FILTER_ROWS;
SELECT CELLS col1:/^w[^a-zA-Z]*$/ from RegexpTest WHERE (ROW = 'suitability' OR ROW = 'moss berry' OR ROW = 'orange marmalade' OR ROW = 'http://yahoo.com') SCAN_AND_FILTER_ROWS;
SELECT * from RegexpTest WHERE (ROW = 'suitability' OR ROW = 'Suitability' OR ROW = 'moss berry' OR ROW = 'moss abc') SCAN_AND_FILTER_ROWS;
SELECT col1:"", col2:"" from RegexpTest WHERE (ROW = 'suitability' OR ROW = 'http://yahoo.com') SCAN_AND_FILTER_ROWS;
SELECT CELLS col1:"" from RegexpTest WHERE (ROW = 'suitability' OR ROW = 'Suitability' OR ROW = 'suitability') SCAN_AND_FILTER_ROWS;
SELECT * from RegexpTest WHERE (ROW = 'Suitability' OR ROW = 'moss Berry' OR ROW = 'Orange marmalade' OR ROW = 'http://yahoo.com/mail') SCAN_AND_FILTER_ROWS;
