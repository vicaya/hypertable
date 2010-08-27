use '/';
drop table if exists RandomTest;
create table RandomTest (
  Field,
  ACCESS GROUP default bloomfilter='rows --false-positive 0.01'
) COMPRESSOR="none";
