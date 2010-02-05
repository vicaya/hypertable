#!/bin/bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`

ulimit -n 1024

$HT_HOME/bin/start-test-servers.sh --clear --no-rangeserver --no-thriftbroker

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.RangeServer.Range.SplitSize=10K \
    --Hypertable.RangeServer.Range.MetadataSplitSize=2K \
    --Hypertable.RangeServer.MaintenanceThreads=8 \
    --Hypertable.RangeServer.Maintenance.Interval=100 > rangeserver.output &

sleep 4

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht ht_load_generator update --spec-file=data.spec --max-bytes=500K

sleep 5

kill -9 `cat $PIDFILE`

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.RangeServer.Range.SplitSize=10K \
    --Hypertable.RangeServer.Range.MetadataSplitSize=2K \
    --Hypertable.RangeServer.MaintenanceThreads=8 \
    --Hypertable.RangeServer.Maintenance.Interval=100 >> rangeserver.output &

$HT_HOME/bin/ht ht_load_generator update --spec-file=data.spec --max-bytes=500K

fgrep ERROR rangeserver.output

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

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/select-all.hql > select.1

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/dump-table.hql > dump.tsv

kill -9 `cat $PIDFILE`

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE >> rangeserver.output &

echo "DROP TABLE IF EXISTS LoadTest; CREATE TABLE LoadTest ( Field );" | $HT_HOME/bin/ht shell --batch

echo "LOAD DATA INFILE 'dump.tsv' INTO TABLE LoadTest;" | $HT_HOME/bin/ht shell --batch

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/select-all.hql > select.2

diff select.1 select.2

if [ $? != 0 ] ; then
    kill -9 `cat $PIDFILE`
    exit 1
fi

kill -9 `cat $PIDFILE`
exit 0
