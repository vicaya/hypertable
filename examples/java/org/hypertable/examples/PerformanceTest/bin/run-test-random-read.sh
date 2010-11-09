#!/usr/bin/env bash

. ~/.ssh-agent

HADOOP_HOME=/opt/hadoop/current
HYPERTABLE_HOME=/opt/hypertable/current
CONFIG=$HYPERTABLE_HOME/conf/hypertable.cfg
HBASE_HOME=/opt/hbase/current
REPORT_DIR=/root/reports
TEST_NAME=test1
WRITE_MULTIPLIER="-S client_multiplier=6"
READ_MULTIPLIER="-S client_multiplier=8"
SCAN_MULTIPLIER="-S client_multiplier=6"

let DATA_SIZE=80000000000
let RANDOM_READ_KEYS=(DATA_SIZE/4)/1000

usage() {
  echo ""
  echo "usage: run-test.sh <system>"
  echo ""
  echo "This script is used to run a performance benchmark.  The <system>"
  echo "argument indicates which system to run the test against and"
  echo "can take a value of 'hypertable' or 'hbase'"
  echo ""
}

clean_hypertable() {
    cap stop_test
    cap -S additional_args="--force" stop
    cap cleandb
    echo "Pausing for 60 seconds ..."
    sleep 60
}

restart_hypertable() {
    clean_hypertable
    cap start
    echo "use '/'; create table perftest ( column ) COMPRESSOR=\"none\";" | $HYPERTABLE_HOME/bin/ht shell --config=$CONFIG
}

clean_hbase() {
    $HBASE_HOME/bin/stop-hbase.sh
    $HADOOP_HOME/bin/hadoop fs -rmr /hbase
    echo "Pausing for 60 seconds ..."
    sleep 60
}

restart_hbase() {
    clean_hbase
    $HBASE_HOME/bin/start-hbase.sh
    echo "create 'perftest', {NAME => 'column'}" | $HBASE_HOME/bin/hbase shell
}

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

SYSTEM=$1
shift

RSTART_SYSTEM=
CLEAN_SYSTEM=

if [ "$SYSTEM" == "hypertable" ] ; then
    RESTART_SYSTEM=restart_hypertable
    CLEAN_SYSTEM=clean_hypertable
elif [ "$SYSTEM" == "hbase" ] ; then
    RESTART_SYSTEM=restart_hbase
    CLEAN_SYSTEM=clean_hbase
else
    echo "ERROR:  Unrecognized system name '$SYSTEM'"
    exit 1
fi

let dsize=DATA_SIZE
while (($dsize >= 2500000000)) ; do
    let keycount=dsize/1000
    ${RESTART_SYSTEM}
    cap -S test_driver=$SYSTEM -S client_multiplier=4 -S test_args="--randomize-tasks --test-name=$TEST_NAME --output-dir=$REPORT_DIR write $keycount 1000" run_test 
    echo "Pausing for 60 seconds ..."
    sleep 60
    cap -S test_driver=$SYSTEM $READ_MULTIPLIER -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random --submit-exactly=$RANDOM_READ_KEYS read $keycount 1000" run_test 
    cap -S test_driver=$SYSTEM $READ_MULTIPLIER -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random --zipf --submit-exactly=$RANDOM_READ_KEYS read $keycount 1000" run_test 
    let dsize=dsize/2
done

${CLEAN_SYSTEM}

