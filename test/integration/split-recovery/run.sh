#!/bin/sh

HYPERTABLE_HOME=~/hypertable/current

# Runs an individual test
run_test() {
    local TEST_ID=$1

    shift;

    $HYPERTABLE_HOME/bin/clean-database.sh 
    $HYPERTABLE_HOME/bin/start-all-servers.sh --no-rangeserver local

    ./rangeserver-launcher.sh $@ &

    sleep 2

    $HYPERTABLE_HOME/bin/hypertable --no-prompt < query-log-create.hql
    if [ $? != 0 ] ; then
        echo "Unable to create table 'query-log', exiting ..."
        exit 1
    fi

    $HYPERTABLE_HOME/bin/hypertable --no-prompt < load.hql
    if [ $? != 0 ] ; then
        echo "Problem loading table 'query-log', exiting ..."
        exit 1
    fi

    $HYPERTABLE_HOME/bin/hypertable --batch < dump-query-log.hql | fgrep -v " INFO " > dbdump.$TEST_ID
    if [ $? != 0 ] ; then
        echo "Problem dumping table 'query-log', exiting ..."
        exit 1
    fi

    cat dbdump.$TEST_ID | wc -l > count.output

    diff count.output count.golden > out
    if [ $? != 0 ] ; then
        echo "Test $TEST_ID FAILED." >> report.txt
        cat out >> report.txt
    else
        echo "Test $TEST_ID PASSED." >> report.txt
    fi
}

rm -f report.txt
rm -f rangeserver.output

##
## TEST 0 - baseline
##

$HYPERTABLE_HOME/bin/clean-database.sh 
rm -rf $HYPERTABLE_HOME/log/*
$HYPERTABLE_HOME/bin/start-all-servers.sh local

$HYPERTABLE_HOME/bin/hypertable --no-prompt < query-log-create.hql
if [ $? != 0 ] ; then
    echo "Unable to create table 'query-log', exiting ..."
    exit 1
fi

sleep 2

$HYPERTABLE_HOME/bin/hypertable --no-prompt < load.hql
if [ $? != 0 ] ; then
    echo "Problem loading table 'query-log', exiting ..."
    exit 1
fi

sleep 5

#
# Stop rangeserver
#
PIDFILE=$HYPERTABLE_HOME/run/Hypertable.RangeServer.pid
if [ -f $pidfile ] ; then
    kill -9 `cat $PIDFILE`
    rm $PIDFILE
fi

# restart range server
$HYPERTABLE_HOME/bin/Hypertable.RangeServer --pidfile=$PIDFILE --verbose > rangeserver.output.0 &

$HYPERTABLE_HOME/bin/hypertable --batch < dump-query-log.hql | fgrep -v " INFO " > dbdump.0
if [ $? != 0 ] ; then
    echo "Problem dumping table 'query-log', exiting ..."
    exit 1
fi

cat dbdump.0 | wc -l > count.output

diff count.output count.golden > out
if [ $? != 0 ] ; then
    echo "Test 0 FAILED." >> report.txt
    cat out >> report.txt
    mv dbdump dbdump.0
else
    echo "Test 0 PASSED." >> report.txt
fi


run_test 1 "--crash-test=split-1:0 --Hypertable.RangeServer.Range.SplitOff high";
run_test 2 "--crash-test=split-2:0 --Hypertable.RangeServer.Range.SplitOff high";
run_test 3 "--crash-test=split-4:0 --Hypertable.RangeServer.Range.SplitOff high";
run_test 4 "--crash-test=split-1:0 --Hypertable.RangeServer.Range.SplitOff low";
run_test 5 "--crash-test=split-2:0 --Hypertable.RangeServer.Range.SplitOff low";
run_test 6 "--crash-test=split-4:0 --Hypertable.RangeServer.Range.SplitOff low";

echo ""
echo "**** TEST REPORT ****"
echo ""
cat report.txt

