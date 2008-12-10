#!/bin/sh

HYPERTABLE_HOME=~/hypertable/current

$HYPERTABLE_HOME/bin/clean-database.sh
$HYPERTABLE_HOME/bin/start-all-servers.sh local --config=hypertable.cfg

$HYPERTABLE_HOME/bin/hypertable --no-prompt < create-table.hql

echo "======================="
echo "Row overflow WRITE test"
echo "======================="
$HYPERTABLE_HOME/bin/ht_write_test --max-keys=1 100000000

