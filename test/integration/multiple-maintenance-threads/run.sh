#!/bin/sh

HYPERTABLE_HOME=~/hypertable/current

$HYPERTABLE_HOME/bin/clean-database.sh
$HYPERTABLE_HOME/bin/start-all-servers.sh local \
    --Hypertable.RangeServer.Range.MaxBytes=10000000 \
    --Hypertable.RangeServer.AccessGroup.MaxMemory=1000000 \
    --Hypertable.RangeServer.MaintenanceThreads=4

$HYPERTABLE_HOME/bin/hypertable --no-prompt < create-table.hql

echo "=================================="
echo "Four Maintenence Thread WRITE test"
echo "=================================="
$HYPERTABLE_HOME/bin/random_write_test 250000000
