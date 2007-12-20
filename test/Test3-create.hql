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
