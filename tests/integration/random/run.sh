#!/usr/bin/env bash

SCRIPT_DIR=`dirname $0`

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}

$HT_HOME/bin/clean-database.sh
$HT_HOME/bin/start-all-servers.sh local

$HT_HOME/bin/ht hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

echo "================="
echo "random WRITE test"
echo "================="
$HT_HOME/bin/ht random_write_test 250000000

#echo -n "Sleeping for 60 seconds ... "
#sleep 60
#echo "done"

echo "================="
echo "random READ test"
echo "================="
$HT_HOME/bin/ht random_read_test 250000000

pushd .
cd $HT_HOME
./bin/clean-database.sh 
./bin/start-all-servers.sh local
popd

$HT_HOME/bin/ht hypertable --no-prompt < $SCRIPT_DIR/create-table-memory.hql

echo "============================="
echo "random WRITE test (IN_MEMORY)"
echo "============================="
$HT_HOME/bin/ht random_write_test 250000000

#echo -n "Sleeping for 60 seconds ... "
#sleep 60
#echo "done"

echo "============================"
echo "random READ test (IN_MEMORY)"
echo "============================"
$HT_HOME/bin/ht random_read_test 250000000

