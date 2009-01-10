#!/bin/bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"100000000"}
THREADS=${THREADS:-"8"}

$HT_HOME/bin/clean-database.sh
$HT_HOME/bin/start-all-servers.sh local \
    --Hypertable.RangeServer.Range.MaxBytes=2500K \
    --Hypertable.RangeServer.AccessGroup.MaxMemory=250K \
    --Hypertable.RangeServer.MaintenanceThreads=$THREADS \
    --Hypertable.RangeServer.Timer.Interval=10k

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

echo "=================================="
echo "$THREADS Maintenence Threads WRITE test"
echo "=================================="
$HT_HOME/bin/random_write_test $DATA_SIZE

dump_it() {
  $HT_HOME/bin/hypertable --batch < $SCRIPT_DIR/dump-table.hql | wc -l
}

count=`dump_it`
expected=$((DATA_SIZE / 1000))

if test $count -eq $expected; then
  exit 0 # success
else
  echo "Expected: $expected, got: $count"
  # try dump again to see if it's a transient problem
  echo "Second dump: `dump_it`"
  exit 1
fi
