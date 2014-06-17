#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
#DATA_SEED=42 # for repeating certain runs
DIGEST="openssl dgst -md5"
GEN_SIZE=${DATA_SIZE:-"1200000"}

. $HT_HOME/bin/ht-env.sh

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
  sleep 3;
  $HT_SHELL --batch < $SCRIPT_DIR/create-test-table.hql
  if [ $? != 0 ] ; then
    echo "Unable to create table 'LoadTest', exiting ..."
    exit 1
  fi

  $HT_HOME/bin/ht ht_load_generator update --no-log-sync --parallel=300 --spec-file=$SCRIPT_DIR/data.spec --max-bytes=$GEN_SIZE
  if [ $? != 0 ] ; then
    echo "Problem loading table 'LoadTest', exiting ..."
    exit 1
  fi

  sleep 0.25

  $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql | grep -v "hypertable" > dbdump.$TEST_ID
  if [ $? != 0 ] ; then
    echo "Problem dumping table 'LoadTest', exiting ..."
    exit 1
  fi

  if [ "$TEST_0" ] ; then
    $DIGEST < dbdump.$TEST_ID > golden.md5
  fi

  $DIGEST < dbdump.$TEST_ID > dbdump.md5
  diff golden.md5 dbdump.md5 > out
  if [ $? != 0 ] ; then
    echo "Test $TEST_ID FAILED." >> report.txt
    echo "Test $TEST_ID FAILED." >> errors.txt
    DUMP_FILE=`pwd`/rsdump.$TEST_ID
    echo "dump '$DUMP_FILE';" | $HT_HOME/bin/ht ht_rsclient --batch

    cat out >> report.txt
    touch error
    $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql | grep -v "hypertable" > dbdump.$TEST_ID.again
    if [ $? != 0 ] ; then
        echo "Problem dumping table 'LoadTest', exiting ..."
        exit 1
    fi
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

env | grep '^TEST_[0-9]=' || set_tests 0 1 2 3 4 5 6 7 8

[ "$TEST_0" ] && run_test 0
[ "$TEST_1" ] && run_test 1 "--induce-failure=split-1:exit:0 \
    --Hypertable.RangeServer.Range.SplitOff high"
[ "$TEST_2" ] && run_test 2 "--induce-failure=split-2:exit:0 \
    --Hypertable.RangeServer.Range.SplitOff high"
[ "$TEST_3" ] && run_test 3 "--induce-failure=split-3:exit:0 \
    --Hypertable.RangeServer.Range.SplitOff high"
[ "$TEST_4" ] && run_test 4 "--induce-failure=split-4:exit:0 \
    --Hypertable.RangeServer.Range.SplitOff high"
[ "$TEST_5" ] && run_test 5 "--induce-failure=split-1:exit:0 \
    --Hypertable.RangeServer.Range.SplitOff low"
[ "$TEST_6" ] && run_test 6 "--induce-failure=split-2:exit:0 \
    --Hypertable.RangeServer.Range.SplitOff low"
[ "$TEST_7" ] && run_test 7 "--induce-failure=split-3:exit:0 \
    --Hypertable.RangeServer.Range.SplitOff low"
[ "$TEST_8" ] && run_test 8 "--induce-failure=split-4:exit:0 \
    --Hypertable.RangeServer.Range.SplitOff low"

if [ -e errors.txt ] && [ "$TEST_8" ] ; then
    ARCHIVE_DIR="archive-"`date | sed 's/ /-/g'`
    mkdir $ARCHIVE_DIR
    mv core.* dbdump.* rangeserver.output.* rsdump.* errors.txt $ARCHIVE_DIR
fi

echo ""
echo "**** TEST REPORT ****"
echo ""
cat report.txt
grep FAILED report.txt > /dev/null && exit 1
exit 0
