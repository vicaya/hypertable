#!/bin/sh

HYPERTABLE_HOME=~/hypertable/current

rm report.txt

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
$HYPERTABLE_HOME/bin/Hypertable.RangeServer --pidfile=$PIDFILE --config=hypertable.cfg --verbose > rangeserver.output.0 &

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


##
## TEST 1 - split 1 crash
##

$HYPERTABLE_HOME/bin/clean-database.sh 
$HYPERTABLE_HOME/bin/start-all-servers.sh --no-rangeserver local

./rangeserver-launcher.sh --crash-test=split-1:0 &

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

$HYPERTABLE_HOME/bin/hypertable --batch < dump-query-log.hql | fgrep -v " INFO " > dbdump.1
if [ $? != 0 ] ; then
    echo "Problem dumping table 'query-log', exiting ..."
    exit 1
fi

cat dbdump.1 | wc -l > count.output

diff count.output count.golden > out
if [ $? != 0 ] ; then
    echo "Test 1 FAILED." >> report.txt
    cat out >> report.txt
    mv dbdump dbdump.1
else
    echo "Test 1 PASSED." >> report.txt
fi


##
## TEST 2 - split 2 crash
##

$HYPERTABLE_HOME/bin/clean-database.sh 
$HYPERTABLE_HOME/bin/start-all-servers.sh --no-rangeserver local

./rangeserver-launcher.sh --crash-test=split-2:0 &

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

$HYPERTABLE_HOME/bin/hypertable --batch < dump-query-log.hql | fgrep -v " INFO " > dbdump.2
if [ $? != 0 ] ; then
    echo "Problem dumping table 'query-log', exiting ..."
    exit 1
fi

cat dbdump.2 | wc -l > count.output

diff count.output count.golden > out
if [ $? != 0 ] ; then
    echo "Test 2 FAILED." >> report.txt
    cat out >> report.txt
    mv dbdump dbdump.2
else
    echo "Test 2 PASSED." >> report.txt
fi

echo ""
echo "**** TEST REPORT ****"
echo ""
cat report.txt

