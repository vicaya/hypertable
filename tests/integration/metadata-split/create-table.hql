use '/';
drop table if exists LoadTest;
CREATE TABLE COMPRESSOR="none" LoadTest (
  Field,
  ACCESS GROUP default BLOCKSIZE=1000 (Field)
);
