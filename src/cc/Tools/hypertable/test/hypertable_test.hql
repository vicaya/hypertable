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
quit
