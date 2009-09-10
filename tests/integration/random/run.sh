#!/bin/sh -v

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"100000000"}

restart_servers() {
  $HT_HOME/bin/start-test-servers.sh --clear "$@"
}

restart_servers "--Hypertable.RangeServer.Range.SplitSize=2000000"

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

echo "================="
echo "random WRITE test"
echo "================="
$HT_HOME/bin/random_write_test $DATA_SIZE

echo "================="
echo "random READ test"
echo "================="
$HT_HOME/bin/random_read_test $DATA_SIZE

restart_servers "--Hypertable.RangeServer.Range.SplitSize=2000000"

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table-memory.hql

echo "============================="
echo "random WRITE test (IN_MEMORY)"
echo "============================="
$HT_HOME/bin/random_write_test $DATA_SIZE

echo "============================"
echo "random READ test (IN_MEMORY)"
echo "============================"
$HT_HOME/bin/random_read_test $DATA_SIZE

restart_servers
