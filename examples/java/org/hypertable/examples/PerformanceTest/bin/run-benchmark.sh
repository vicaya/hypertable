#!/usr/bin/env bash

. ~/.ssh-agent

HADOOP_HOME=/opt/hadoop/current
HYPERTABLE_HOME=/opt/hypertable/current
HBASE_HOME=/opt/hbase/current
REPORT_DIR=/home/doug/benchmark
TEST_NAME=test1
let DATA_SIZE=80000000000


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
    cap stop
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

let vsize=10000
while (($vsize >= 10)) ; do
    let keycount=DATA_SIZE/vsize
    ${RESTART_SYSTEM}
    cap -S test_driver=$SYSTEM -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR write $keycount $vsize" run_test 
    ${RESTART_SYSTEM}
    cap -S test_driver=$SYSTEM -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random write $keycount $vsize" run_test 
    echo "Pausing for 60 seconds ..."
    sleep 60
    cap -S test_driver=$SYSTEM -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR scan $keycount $vsize" run_test 
    cap -S test_driver=$SYSTEM -S client_multiplier=8 -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR read $keycount $vsize" run_test 
    let vsize=vsize/10
done


while (($DATA_SIZE >= 10000000000)) ; do
    let keycount=DATA_SIZE/1000
    ${RESTART_SYSTEM}    
    cap -S test_driver=$SYSTEM -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random write $keycount 1000" run_test 
    echo "Pausing for 60 seconds ..."
    sleep 60
    cap -S test_driver=$SYSTEM -S client_multiplier=8 -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random read $keycount 1000" run_test 
    cap -S test_driver=$SYSTEM -S client_multiplier=8 -S test_args="--test-name=$TEST_NAME --output-dir=$REPORT_DIR --random --zipf read $keycount 1000" run_test 
    let DATA_SIZE=DATA_SIZE/2
done

${CLEAN_SYSTEM}

#1. 64GB sequential-write: 10K, 1K, 100, 10
#2. 64GB random-write: 10K, 1K, 100, 10
#3. 64GB sequential-read: 10K, 1K, 100, 10
#4. random-read uniform 32 clients 1KB: 64GB, 32GB, 16GB, 4GB
#5. random-read zipfian 32 clients 1KB: 64GB, 32GB, 16GB, 4GB
#1. sequential-write, random-write, sequential-read, scan 10K
#2. sequential-write, random-write, sequential-read, scan 1K
#3. sequential-write, random-write, sequential-read, scan 100
#4. sequential-write, random-write, sequential-read, scan 10
#5. 64GB random read uniform + zipfian
#6. 32GB random read uniform + zipfian
#7. 16GB random read uniform + zipfian
#8. 4GB random read uniform + zipfian

