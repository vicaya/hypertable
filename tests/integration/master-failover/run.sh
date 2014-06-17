#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
HT_SHELL=$HT_HOME/bin/hypertable
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`
startlog=/tmp/start-Master$$.log
let testnum=1

. $HT_HOME/bin/ht-env.sh

start_masters() {

    kill -9 `cat $HT_HOME/run/Hypertable.Master*.pid` 2>&1 >& /dev/null

    $HT_HOME/bin/Hypertable.Master --Hypertable.Master.Port=38050 $@ \
        --pidfile $HT_HOME/run/Hypertable.Master.38050.pid \
        --Hypertable.Connection.Retry.Interval=3000 2>&1 &> Hypertable.Master.38050.log&
    wait_for_server_up "master" "master:38050" "--Hypertable.Master.Port=38050"

    $HT_HOME/bin/Hypertable.Master --Hypertable.Master.Port=38051 \
        --pidfile $HT_HOME/run/Hypertable.Master.38051.pid \
        --Hypertable.Connection.Retry.Interval=3000 2>&1 &> Hypertable.Master.38051.log&
    wait_for_server_up "master" "master:38051" "--Hypertable.Master.Port=38051"
}

run_test() {
    echo -n "Running test $testnum \"$@\" ... "
    echo $@ | $HT_HOME/bin/ht shell --batch --Hypertable.Connection.Retry.Interval=3000
    if [ $? != 0 ] ; then
        echo "failure, exiting...";
        exit 1
    fi
    echo "success"
    let testnum++
}


$HT_HOME/bin/start-test-servers.sh --no-thriftbroker --clear

echo "use '/'; create namespace test;" | $HT_HOME/bin/ht shell --batch

kill -9 `cat $HT_HOME/run/Hypertable.Master*.pid`

start_masters "--induce-failure=connection-handler-before-id-response:exit:1"
run_test "use '/test'; create namespace foo;"

start_masters "--induce-failure=create-namespace-ASSIGN_ID-c:exit:0"
run_test "use '/test/foo'; create namespace bar;"

start_masters "--induce-failure=connection-handler-before-id-response:exit:1"
run_test "drop namespace '/test/foo/bar';"

start_masters "--induce-failure=drop-namespace-STARTED-b:exit:1"
run_test "drop namespace '/test/foo';"

start_masters "--induce-failure=connection-handler-before-id-response:exit:1"
run_test "USE '/test'; CREATE TABLE how ( a );"

start_masters "--induce-failure=create-table-LOAD_RANGE-a:exit:1"
run_test "USE '/test'; CREATE TABLE to ( a );"

start_masters "--induce-failure=connection-handler-before-id-response:exit:1"
run_test "USE '/test'; RENAME TABLE how TO win;"

start_masters "--induce-failure=rename-table-STARTED:exit:1"
run_test "USE '/test'; RENAME TABLE to TO friends;"

start_masters "--induce-failure=connection-handler-before-id-response:exit:1"
run_test "USE '/test'; ALTER TABLE win ADD ( b );"

start_masters "--induce-failure=alter-table-INITIAL:exit:1"
run_test "USE '/test'; ALTER TABLE friends ADD ( b );"

start_masters "--induce-failure=connection-handler-before-id-response:exit:1"
run_test "USE '/test'; DROP TABLE win;"

start_masters "--induce-failure=drop-table-INITIAL:exit:1"
run_test "USE '/test'; DROP TABLE friends;"

kill -9 `cat $HT_HOME/run/Hypertable.Master.*.pid`
$HT_HOME/bin/clean-database.sh 

exit 0
