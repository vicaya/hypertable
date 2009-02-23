drop table if exists RandomTest;
create table COMPRESSOR="none" RandomTest (
  Field,
  ACCESS GROUP default bloomfilter='rows+cols --false-positive 0.05'
);
