use '/';
drop table if exists LoadTest;
CREATE TABLE LoadTest (
  Field,
  ACCESS GROUP default BLOCKSIZE=1000 (Field)
) COMPRESSOR="none";
