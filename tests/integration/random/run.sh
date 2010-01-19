#!/bin/sh

SCRIPT_DIR=`dirname $0`

HYPERTABLE_HOME=/opt/hypertable/current

$HYPERTABLE_HOME/bin/clean-database.sh
$HYPERTABLE_HOME/bin/start-all-servers.sh local

$HYPERTABLE_HOME/bin/ht hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

echo "================="
echo "random WRITE test"
echo "================="
$HYPERTABLE_HOME/bin/ht random_write_test 250000000

#echo -n "Sleeping for 60 seconds ... "
#sleep 60
#echo "done"

echo "================="
echo "random READ test"
echo "================="
$HYPERTABLE_HOME/bin/ht random_read_test 250000000

pushd .
cd $HYPERTABLE_HOME
./bin/clean-database.sh 
./bin/start-all-servers.sh local
popd

$HYPERTABLE_HOME/bin/ht hypertable --no-prompt < $SCRIPT_DIR/create-table-memory.hql

echo "============================="
echo "random WRITE test (IN_MEMORY)"
echo "============================="
$HYPERTABLE_HOME/bin/ht random_write_test 250000000

#echo -n "Sleeping for 60 seconds ... "
#sleep 60
#echo "done"

echo "============================"
echo "random READ test (IN_MEMORY)"
echo "============================"
$HYPERTABLE_HOME/bin/ht random_read_test 250000000

