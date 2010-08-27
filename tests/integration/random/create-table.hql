use '/';
drop table if exists RandomTest;
create table RandomTest (
  Field,
  T1,
  T2,
  T3,
  T4,
  T5,
  T6,
  T7,
  ACCESS GROUP default (Field),
  ACCESS GROUP AT1 (T1),
  ACCESS GROUP AT2 (T2),
  ACCESS GROUP AT3 (T3),
  ACCESS GROUP AT4 (T4),
  ACCESS GROUP AT5 (T5),
  ACCESS GROUP AT6 (T6),
  ACCESS GROUP AT7 (T7)
) COMPRESSOR="none";
