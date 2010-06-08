CREATE TABLE HiveTest('address');
INSERT INTO HiveTest VALUES('1', 'address:home', '1 Home St');
INSERT INTO HiveTest VALUES('1', 'address:office', '1 Office St');
INSERT INTO HiveTest VALUES('2', 'address:nyc', '2 Foo Way');
INSERT INTO HiveTest VALUES('3', 'address:restaurant', '3 Bar Ct');
INSERT INTO HiveTest VALUES('3', 'address:home', '3 Home Ct');
INSERT INTO HiveTest VALUES('4', 'address', '4 My only address place');
SELECT * FROM HiveTest;
