CREATE TABLE Sample (
apple,
banana,
onion,
cassis,
cherry,
yam,
plum,
broccoli,
ACCESS GROUP default ( apple, banana ),
ACCESS GROUP marsha ( onion, cassis ),
ACCESS GROUP jan ( cherry ),
ACCESS GROUP cindy ( yam, plum, broccoli )
);
CREATE TABLE Sample_tmp LIKE Sample;
