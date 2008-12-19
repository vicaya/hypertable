#!/bin/sh

HYPERTABLE_HOME=~/hypertable/current

$HYPERTABLE_HOME/bin/clean-database.sh

$HYPERTABLE_HOME/bin/start-all-servers.sh local --config=hypertable.cfg

$HYPERTABLE_HOME/bin/hypertable --no-prompt < create-table.hql

echo "================================="
echo "Two Maintenence Thread WRITE test"
echo "================================="
$HYPERTABLE_HOME/bin/random_write_test 250000000

while [ true ]; do
  ls -l $HYPERTABLE_HOME/fs/local/hypertable/servers/10.0.30.100_38060/log/user/
  sleep 2
done
