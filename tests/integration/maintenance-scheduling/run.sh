#!/bin/bash

# Run this on an AWS small instance and make sure the output looks similar
# to that contained in 'report.sample' in terms of the changes in memory
# allocation

HT_HOME=/mnt/hypertable/current
HYPERTABLE_HOME=$HT_HOME
SCRIPT_DIR=`dirname $0`

$HT_HOME/bin/clean-database.sh
$HT_HOME/bin/start-all-servers.sh --no-thriftbroker local

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/create-table.hql
if [ $? != 0 ] ; then
    echo "Unable to create table 'LoadTest', exiting ..."
    exit 1
fi

$HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec --max-bytes=3G
if [ $? != 0 ] ; then
    echo "Problem loading table 'LoadTest', exiting ..."
    exit 1
fi

sleep 120

rm -f report.txt
echo "=============" >> report.txt
echo "== LOADING ==" >> report.txt
echo "=============" >> report.txt
fgrep "Memory Allocation" $HT_HOME/log/Hypertable.RangeServer.log | fgrep -v Usage | tail -10 | cut -b123- >> report.txt


$HT_HOME/bin/ht ht_load_generator query --spec-file=./data.spec --max-keys=20000

echo "==============" >> report.txt
echo "== QUERYING ==" >> report.txt
echo "==============" >> report.txt
fgrep "Memory Allocation" $HT_HOME/log/Hypertable.RangeServer.log | fgrep -v Usage | tail -20 | cut -b123- >> report.txt

cat report.txt

