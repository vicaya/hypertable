use '/';
drop table if exists RandomTest;
create table RandomTest (
  Field,
  ACCESS GROUP default bloomfilter='rows+cols --false-positive 0.05'
) COMPRESSOR="none";
