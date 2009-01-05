#!/bin/sh

HYPERTABLE_HOME=~/hypertable/current

$HYPERTABLE_HOME/bin/clean-database.sh
$HYPERTABLE_HOME/bin/start-all-servers.sh local \
    --Hypertable.RangeServer.Range.MaxBytes=2500000 \
    --Hypertable.RangeServer.AccessGroup.MaxMemory=250000 \
    --Hypertable.RangeServer.MaintenanceThreads=8 \
    --Hypertable.RangeServer.Timer.Interval=15000

$HYPERTABLE_HOME/bin/hypertable --no-prompt < create-table.hql

echo "=================================="
echo "Four Maintenence Thread WRITE test"
echo "=================================="
$HYPERTABLE_HOME/bin/random_write_test 250000000

$HYPERTABLE_HOME/bin/hypertable --batch < dump-table.hql > dbdump

wc -l dbdump > count.output
diff count.output count.golden

