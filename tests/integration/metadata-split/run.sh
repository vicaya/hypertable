#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
HT_SHELL=$HT_HOME/bin/hypertable
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`
startlog=/tmp/start-metadata-split$$.log

. $HT_HOME/bin/ht-env.sh

cleanup_and_abort() {
    kill %1
    sleep 10
    if [ -f $PIDFILE ]; then
      kill -9 `cat $PIDFILE`
      rm -f $PIDFILE
    fi
    exit 1
}

start_masters() {

    $HT_HOME/bin/Hypertable.Master --Hypertable.Master.Port=38050 $@ \
        --pidfile $HT_HOME/run/Hypertable.Master.38050.pid \
        --Hypertable.Connection.Retry.Interval=3000 2>&1 &> Hypertable.Master.38050.log&
    wait_for_server_up "master" "master:38050" "--Hypertable.Master.Port=38050"

    $HT_HOME/bin/Hypertable.Master --Hypertable.Master.Port=38051 \
        --pidfile $HT_HOME/run/Hypertable.Master.pid \
        --Hypertable.Connection.Retry.Interval=3000 2>&1 &> Hypertable.Master.38051.log&
    wait_for_server_up "master" "master:38051" "--Hypertable.Master.Port=38051"
}


stop_range_server() {
  # stop any existing range server if necessary
  if [ -f $PIDFILE ]; then
    kill -9 `cat $PIDFILE`
    rm -f $PIDFILE
    sleep 3

    if $HT_HOME/bin/ht serverup --silent rangeserver; then
      echo "Can't stop range server, exiting..."
      ps -ef | grep Hypertable.RangeServer
      exit 1
    fi
  fi
}

set_tests() {
  for i in $@; do
    eval TEST_$i=1
  done
}

save_failure_state() {
  echo "USE sys; SELECT * FROM METADATA RETURN_DELETES DISPLAY_TIMESTAMPS;" | $HT_HOME/bin/ht shell --batch >& metadata.dump
  tar czvf fs-backup.tgz $HT_HOME/fs/local
  ARCHIVE_DIR="archive-"`date | sed 's/ /-/g'`
  mkdir $ARCHIVE_DIR
  mv metadata.dump fs-backup.tgz core.* select* dump.tsv rangeserver.output* error* failed* running* $ARCHIVE_DIR
  cp $HT_HOME/log/Hypertable.Master.log $ARCHIVE_DIR
  if [ -e Testing/Temporary/LastTest.log.tmp ] ; then
    mv Testing/Temporary/LastTest.log.tmp $ARCHIVE_DIR
  elif [ -e ../../../Testing/Temporary/LastTest.log.tmp ] ; then
    mv ../../../Testing/Temporary/LastTest.log.tmp $ARCHIVE_DIR
  fi
  echo "Failure state saved to directory $ARCHIVE_DIR"
  #exec 1>&-
  #sleep 86400
}

# Runs an individual test
run_test() {
  local TEST_ID=$1
  shift;

  touch running.$TEST_ID

  if [ -z "$SKIP_START_SERVERS" ]; then
    $HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker \
                                       --clear --Hypertable.Master.Gc.Interval=30000
  fi

  if [ $TEST_ID == 10 ] || [ $TEST_ID == 11 ] || [ $TEST_ID == 12 ] || [ $TEST_ID == 13 ] ; then
      kill -9 `cat $HT_HOME/run/Hypertable.Master*.pid`
      stop_range_server
      start_masters $@
      $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
          --Hypertable.RangeServer.Range.SplitSize=25K \
          --Hypertable.RangeServer.CellStore.DefaultBlockSize=1K \
          --Hypertable.RangeServer.Range.MetadataSplitSize=10K \
          --Hypertable.RangeServer.MaintenanceThreads=8 \
          --Hypertable.RangeServer.Maintenance.Interval=100 2>&1 &> rangeserver.output &
  else
      stop_range_server
      $SCRIPT_DIR/rangeserver-launcher.sh $@ > rangeserver.output.$TEST_ID 2>&1 &
  fi

  # give servers time to come up
  sleep 2;

  $HT_SHELL --batch < $SCRIPT_DIR/create-table.hql
  if [ $? != 0 ] ; then
      # try again
      $HT_SHELL --batch < $SCRIPT_DIR/create-table.hql
      if [ $? != 0 ] ; then
          echo "Error creating table 'LoadTest', shell returned %?"
          save_failure_state
          cleanup_and_abort
      fi
  fi

  $HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec --max-bytes=300K \
      --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K

  if [ $? != 0 ] ; then
      echo "Error loading table 'LoadTest', shell returned $?"
      save_failure_state
      cleanup_and_abort 
  fi

  sleep 1
  echo "wait for maintenance;" | $HT_HOME/bin/ht ht_rsclient --batch  

  fgrep ERROR rangeserver.output.$TEST_ID | fgrep -v FailureInducer | fgrep -v skew > error.$TEST_ID

  if [ -s error.$TEST_ID ] ; then
      touch failed.$TEST_ID
      save_failure_state
      cleanup_and_abort 
  fi

  if [ -e core.* ] ; then
      echo "ERROR: core file generated"
      touch failed.$TEST_ID
      save_failure_state
      cleanup_and_abort 
  fi

  /bin/rm -f select-a.$TEST_ID select-b.$TEST_ID

  echo "USE '/'; SELECT * FROM LoadTest INTO FILE 'select-a.$TEST_ID';" | $HT_HOME/bin/ht shell --batch

  /bin/rm -f dump.tsv 

  echo "USE '/'; DUMP TABLE LoadTest INTO FILE 'dump.tsv';" | $HT_HOME/bin/ht shell --batch

  echo "USE '/'; DROP TABLE IF EXISTS LoadTest; CREATE TABLE LoadTest ( Field );" | $HT_HOME/bin/ht shell --batch

  echo "USE '/'; LOAD DATA INFILE 'dump.tsv' INTO TABLE LoadTest;" | $HT_HOME/bin/ht shell --batch

  echo "USE '/'; SELECT * FROM LoadTest INTO FILE 'select-b.$TEST_ID';" | $HT_HOME/bin/ht shell --batch

  diff select-a.$TEST_ID select-b.$TEST_ID
  if [ $? != 0 ] ; then
    echo "Test $TEST_ID FAILED." >> report.txt
    echo "Test $TEST_ID FAILED." >> errors.txt
    touch failed.$TEST_ID
  else
    echo "Test $TEST_ID PASSED." >> report.txt
  fi

  /bin/rm -f running.$TEST_ID
}

env | grep '^TEST_[0-9]' || set_tests 0 1 2 3 4 5 6 7 8 9 10 11 12 13

[ "$TEST_0" ] && run_test 0
[ "$TEST_1" ] && run_test 1 "--induce-failure=metadata-split-1:exit:0"
[ "$TEST_2" ] && run_test 2 "--induce-failure=metadata-split-2:exit:0"
[ "$TEST_3" ] && run_test 3 "--induce-failure=metadata-split-3:exit:0"
[ "$TEST_4" ] && run_test 4 "--induce-failure=metadata-split-4:exit:0"
[ "$TEST_5" ] && run_test 5 "--induce-failure=metadata-load-range-1:exit:2"
[ "$TEST_6" ] && run_test 6 "--induce-failure=metadata-load-range-2:exit:2"
[ "$TEST_7" ] && run_test 7 "--induce-failure=metadata-load-range-3:exit:2"
[ "$TEST_8" ] && run_test 8 "--induce-failure=metadata-load-range-4:exit:2"
[ "$TEST_9" ] && run_test 9 "--induce-failure=metadata-load-range-5:exit:2"
[ "$TEST_10" ] && run_test 10 "--induce-failure=connection-handler-before-id-response:exit:2"
[ "$TEST_11" ] && run_test 11 "--induce-failure=move-range-INITIAL-a:exit:0"
[ "$TEST_12" ] && run_test 12 "--induce-failure=move-range-STARTED:exit:0"
[ "$TEST_13" ] && run_test 13 "--induce-failure=move-range-LOAD_RANGE:exit:0"

if [ "$TEST_9" ] ; then
  if [ -e running* ] || [ -e failed* ]; then
    save_failure_state
  fi
  /bin/rm -f core.* select* dump.tsv rangeserver.output.* error* running* failed*
fi


kill %1
if [ -f $PIDFILE ]; then
    kill -9 `cat $PIDFILE`
    rm -f $PIDFILE
fi

echo ""
echo "**** TEST REPORT ****"
echo ""
cat report.txt
grep FAILED report.txt > /dev/null && exit 1
exit 0
