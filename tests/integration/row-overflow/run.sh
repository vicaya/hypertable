#!/bin/sh

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"100000000"}

$HT_HOME/bin/clean-database.sh
$HT_HOME/bin/start-all-servers.sh local \
    --Hypertable.RangeServer.Range.MaxBytes=25000000

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

echo "======================="
echo "Row overflow WRITE test"
echo "======================="
$HT_HOME/bin/ht_write_test --max-keys=1 $DATA_SIZE 2>&1 |
    grep -c '^Failed.*row overflow'
