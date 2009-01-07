#!/bin/bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"100000000"}
THREADS=${THREADS:-"8"}

$HT_HOME/bin/clean-database.sh
$HT_HOME/bin/start-all-servers.sh local \
    --Hypertable.RangeServer.Range.MaxBytes=2500000 \
    --Hypertable.RangeServer.AccessGroup.MaxMemory=250000 \
    --Hypertable.RangeServer.MaintenanceThreads=$THREADS \
    --Hypertable.RangeServer.Timer.Interval=10000

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

echo "=================================="
echo "Four Maintenence Thread WRITE test"
echo "=================================="
$HT_HOME/bin/random_write_test $DATA_SIZE

dc=`$HT_HOME/bin/hypertable --batch < $SCRIPT_DIR/dump-table.hql | wc -l`

test $dc -eq $((DATA_SIZE / 1000))
