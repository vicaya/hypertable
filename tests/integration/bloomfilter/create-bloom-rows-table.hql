drop table if exists RandomTest;
create table COMPRESSOR="none" RandomTest (
  Field,
  ACCESS GROUP default bloomfilter='rows --false-positive 0.01'
);
