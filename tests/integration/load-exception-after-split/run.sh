#!/bin/sh

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"25000000"}

ARGS=""

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.Range.SplitSize=2500K \
    --induce-failure=load-range-1:throw:3

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

echo "=== writing $DATA_SIZE bytes of data ==="
$HT_HOME/bin/random_write_test $ARGS $DATA_SIZE

echo "=== reading $DATA_SIZE bytes of data ==="
$HT_HOME/bin/random_read_test $DATA_SIZE
