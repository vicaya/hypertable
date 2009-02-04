drop table if exists RandomTest;
create table COMPRESSOR="none" RandomTest (
  Field,
  ACCESS GROUP default bloomfilter='ROWS' bloom_false_positive_rate=0.01
);
