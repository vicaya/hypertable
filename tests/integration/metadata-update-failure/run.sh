#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
#DATA_SEED=42 # for repeating certain runs
DIGEST="openssl dgst -md5"
CFG_FILE="${SCRIPT_DIR}/metadata-update-failure.cfg"

. $HT_HOME/bin/ht-env.sh

gen_test_data() {
  seed=${DATA_SEED:-$$}
  size=${DATA_SIZE:-"300"}
  perl -e 'print "#row\tcolumn\tvalue\n"' > data.header
  perl -e 'srand('$seed'); for($i=0; $i<'$size'; ++$i) {
    printf "row%07d\tcolumn%d\tvalue%d\n", $i, int(rand(3))+1, $i
  }' > data.body
  $DIGEST < data.body > data.md5
}

stop_range_server() {
  # stop any existing range server if necessary
  pidfile=$HT_HOME/run/Hypertable.RangeServer.pid
  if [ -f $pidfile ]; then
    kill -9 `cat $pidfile`
    rm -f $pidfile
    sleep 1

    if $HT_HOME/bin/serverup --silent rangeserver; then
      echo "Can't stop range server, exiting"
      ps -ef | grep Hypertable.RangeServer
      exit 1
    fi
  fi
}


run_test() {
  $HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker --clear \
      --config $CFG_FILE 
  stop_range_server

  $HT_HOME/bin/Hypertable.RangeServer --verbose \
      --induce-failure=LiveFileTracker-update_files_column:throw:0\
      --config $SCRIPT_DIR/metadata-update-failure.cfg > rangeserver.output >&1 &
  # give rangeserver time to get registered etc 
  sleep 3;
  
  $HT_SHELL --batch < $SCRIPT_DIR/create-test-table.hql
  if [ $? != 0 ] ; then
    echo "Unable to create table 'metadata-update-failure-test', exiting ..."
    exit 1
  fi

  $HT_SHELL --config $CFG_FILE --batch < $SCRIPT_DIR/load.hql
  if [ $? != 0 ] ; then
    echo "Problem loading table 'metadata-update-failure-test', exiting ..."
    exit 1
  fi

  sleep 0.25

  $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql | grep -v "hypertable" > dbdump
  if [ $? != 0 ] ; then
    echo "Problem dumping table 'metadata-update-failure-test', exiting ..."
    exit 1
  fi

  $DIGEST < dbdump > dbdump.md5
  diff data.md5 dbdump.md5 > out
  if [ $? != 0 ] ; then
    echo "Test FAILED." >> report.txt
    echo "Test FAILED." >> errors.txt
    cat out >> report.txt
    touch error
    $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql | grep -v "hypertable" > dbdump.again
    if [ $? != 0 ] ; then
        echo "Problem dumping table 'metadata-update-failure-test', exiting ..."
        exit 1
    fi
  else
    echo "Test PASSED." >> report.txt
  fi
}

rm -f errors.txt
rm -f report.txt

gen_test_data

run_test 

if [ -e errors.txt ] ; then
    ARCHIVE_DIR="archive-"`date | sed 's/ /-/g'`
    mkdir $ARCHIVE_DIR
    mv core.* dbdump.* rangeserver.output.* errors.txt $ARCHIVE_DIR
fi

echo ""
echo "**** TEST REPORT ****"
echo ""
cat report.txt
$HT_HOME/bin/stop-servers.sh 
echo "Test OVER." >> report.txt

grep FAILED report.txt > /dev/null && exit 1
exit 0
