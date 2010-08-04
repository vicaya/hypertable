#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
HT_SHELL=$HT_HOME/bin/hypertable
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`

. $HT_HOME/bin/ht-env.sh

cleanup_and_abort() {
    echo $1
    kill %1
    if [ -f $PIDFILE ]; then
      kill -9 `cat $PIDFILE`
      rm -f $PIDFILE
    fi
    exit 1
}

stop_range_server() {
  # stop any existing range server if necessary
  if [ -f $PIDFILE ]; then
    kill -9 `cat $PIDFILE`
    rm -f $PIDFILE
    sleep 3

    if $HT_HOME/bin/ht serverup --silent rangeserver; then
      echo "Can't stop range server, exiting"
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

# Runs an individual test
run_test() {
  local TEST_ID=$1
  shift;

  if [ -z "$SKIP_START_SERVERS" ]; then
    $HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker \
                                       --clear
  fi

  stop_range_server

  $SCRIPT_DIR/rangeserver-launcher.sh $@ > rangeserver.output.$TEST_ID 2>&1 &

  # give rangeserver time to get registered etc 
  sleep 2;

  $HT_SHELL --batch < $SCRIPT_DIR/create-table.hql
  if [ $? != 0 ] ; then
      # try again
      $HT_SHELL --batch < $SCRIPT_DIR/create-table.hql
      if [ $? != 0 ] ; then
          cleanup_and_abort "Unable to create table 'LoadTest', exiting ..."
      fi
  fi

  $HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec --max-bytes=500K
  if [ $? != 0 ] ; then
      cleanup_and_abort "Problem loading table 'LoadTest', exiting ..."
  fi

  sleep 5

  fgrep ERROR rangeserver.output.$TEST_ID | fgrep -v FailureInducer | fgrep -v skew > error.$TEST_ID

  fgrep ERROR error.$TEST_ID

  if [ $? == 0 ] ; then
      kill -9 `cat $PIDFILE`
      exit 1
  fi

  if [ -e core.* ] ; then
      echo "core file generated, moved to /tmp"
      mv core.* /tmp
      kill -9 `cat $PIDFILE`
      exit 1
  fi

  echo "USE '/'; SELECT * FROM LoadTest INTO FILE 'select-a.$TEST_ID';" | $HT_HOME/bin/ht shell --batch

  $HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/dump-table.hql > dump.tsv

  echo "USE '/'; DROP TABLE IF EXISTS LoadTest; CREATE TABLE LoadTest ( Field );" | $HT_HOME/bin/ht shell --batch

  echo "USE '/'; LOAD DATA INFILE 'dump.tsv' INTO TABLE LoadTest;" | $HT_HOME/bin/ht shell --batch

  echo "USE '/'; SELECT * FROM LoadTest INTO FILE 'select-b.$TEST_ID';" | $HT_HOME/bin/ht shell --batch

  diff select-a.$TEST_ID select-b.$TEST_ID
  if [ $? != 0 ] ; then
    echo "Test $TEST_ID FAILED." >> report.txt
    echo "Test $TEST_ID FAILED." >> errors.txt
    #exec 1>&-
    #sleep 86400
  else
    echo "Test $TEST_ID PASSED." >> report.txt
  fi
}

if [ "$TEST_0" ] || [ "$TEST_1" ] ; then
    rm -f errors.txt
fi

rm -f report.txt

env | grep '^TEST_[0-9]=' || set_tests 0 1 2 3 4 5 6 7 8 9

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

if [ -e errors.txt ] && [ "$TEST_4" ] ; then
    ARCHIVE_DIR="archive-"`date | sed 's/ /-/g'`
    mkdir $ARCHIVE_DIR
    mv core.* select.* dump.tsv rangeserver.output.* errors.txt $ARCHIVE_DIR
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
