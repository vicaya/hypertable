#!/usr/bin/env bash

. ~/.ssh-agent

HADOOP_HOME=/opt/hadoop/current
HYPERTABLE_HOME=/opt/hypertable/current
HBASE_HOME=/opt/hbase/current
REPORT_DIR=/home/doug/benchmark
TEST_NAME=test1
let DATA_SIZE=80000000000
WRITE_MULTIPLIER="-S client_multiplier=6"
READ_MULTIPLIER="-S client_multiplier=8"

usage() {
  echo ""
  echo "usage: run-benchmark.sh <system>"
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
    echo "create table perftest ( column );" | $HYPERTABLE_HOME/bin/ht shell --config=$HYPERTABLE_HOME/conf/perftest-hypertable.cfg
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
while (($dsize >= 10000000000)) ; do
    let keycount=dsize/1000
    ${RESTART_SYSTEM}
    cap -S test_driver=$SYSTEM $WRITE_MULTIPLIER -S test_args="--randomize-tasks --test-name=$TEST_NAME --output-dir=$REPORT_DIR write $keycount 1000" run_test 
    echo "Pausing for 60 seconds ..."
    sleep 60
    cap -S test_driver=$SYSTEM $READ_MULTIPLIER -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random read $keycount 1000" run_test 
    cap -S test_driver=$SYSTEM $READ_MULTIPLIER -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random --zipf read $keycount 1000" run_test 
    let dsize=dsize/2
done

let maxKeys=DATA_SIZE/100
let vsize=10000
while (($vsize >= 10)) ; do
    let keycount=DATA_SIZE/vsize

    # Sequential write (turned off for now)
    # ${RESTART_SYSTEM}
    # cap -S test_driver=$SYSTEM $WRITE_MULTIPLIER -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR write $keycount $vsize" run_test 

    # Random Write
    ${RESTART_SYSTEM}
    cap -S test_driver=$SYSTEM $WRITE_MULTIPLIER -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random write $keycount $vsize" run_test 

    echo "Pausing for 60 seconds ..."
    sleep 60

    # Scan
    cap -S test_driver=$SYSTEM $READ_MULTIPLIER -S test_args="--max-keys=$maxKeys --test-name=$TEST_NAME --output-dir=$REPORT_DIR scan $keycount $vsize" run_test 

    # Sequential read
    cap -S test_driver=$SYSTEM $READ_MULTIPLIER -S test_args="--max-keys=$maxKeys --test-name=$TEST_NAME --output-dir=$REPORT_DIR read $keycount $vsize" run_test 
    let vsize=vsize/10
done

${CLEAN_SYSTEM}

