USE '/test';
DROP TABLE IF EXISTS Test1;
DROP TABLE IF EXISTS Test2;
DROP TABLE IF EXISTS Test3;
DROP TABLE IF EXISTS Test4;
CREATE TABLE Test1 (
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
CREATE TABLE Test2 (
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
CREATE TABLE Test3 (
apple MAX_VERSIONS=1,
banana MAX_VERSIONS=2,
onion MAX_VERSIONS=3,
cassis MAX_VERSIONS=1,
cherry MAX_VERSIONS=2,
yam MAX_VERSIONS=3,
plum MAX_VERSIONS=5,
broccoli MAX_VERSIONS=1,
ACCESS GROUP default ( apple, banana ),
ACCESS GROUP marsha ( onion, cassis ),
ACCESS GROUP jan IN_MEMORY ( cherry ),
ACCESS GROUP cindy ( yam, plum, broccoli )
);
CREATE TABLE Test4 (
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
