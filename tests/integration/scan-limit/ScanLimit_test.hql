use '/';
drop table if exists ScanLimitTable;
create table ScanLimitTable(
  Field,
  ACCESS GROUP default bloomfilter='rows --false-positive 0.01'
) COMPRESSOR="none";
insert into ScanLimitTable VALUES ("Ensure Hypertable.RangeServer.AccessGroup.MaxMemory",
    'Field', " is less than the size of this row");
insert into ScanLimitTable VALUES ("In which case queries will hit the CellStore", 'Field',
    " and NOT the CellCache, since it is too small");
pause 5;
select * from ScanLimitTable limit=1;
select * from ScanLimitTable limit=2;
