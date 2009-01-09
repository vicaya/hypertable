#!/bin/sh

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"100000000"}

restart_servers() {
  $HT_HOME/bin/clean-database.sh
  $HT_HOME/bin/start-all-servers.sh local
}

restart_servers

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

echo "================="
echo "random WRITE test"
echo "================="
$HT_HOME/bin/random_write_test $DATA_SIZE

echo "================="
echo "random READ test"
echo "================="
$HT_HOME/bin/random_read_test $DATA_SIZE

restart_servers

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table-memory.hql

echo "============================="
echo "random WRITE test (IN_MEMORY)"
echo "============================="
$HT_HOME/bin/random_write_test $DATA_SIZE

echo "============================"
echo "random READ test (IN_MEMORY)"
echo "============================"
$HT_HOME/bin/random_read_test $DATA_SIZE

