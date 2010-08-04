#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`

ulimit -n 1024

$HT_HOME/bin/start-test-servers.sh --clear --no-rangeserver --no-thriftbroker

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.RangeServer.Range.SplitSize=20K \
    --Hypertable.RangeServer.Range.MetadataSplitSize=2K \
    --Hypertable.RangeServer.MaintenanceThreads=8 \
    --Hypertable.RangeServer.Maintenance.Interval=100 > rangeserver.output &

sleep 2

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec --max-bytes=500K

sleep 5

kill -9 `cat $PIDFILE`

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.RangeServer.Range.SplitSize=20K \
    --Hypertable.RangeServer.Range.MetadataSplitSize=2K \
    --Hypertable.RangeServer.MaintenanceThreads=8 \
    --Hypertable.RangeServer.Maintenance.Interval=100 >> rangeserver.output &

$HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec --max-bytes=500K

fgrep ERROR rangeserver.output
fgrep ERROR rangeserver.output | fgrep -v " skew " > error.0

fgrep ERROR error.0

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

echo "USE '/'; SELECT * FROM LoadTest INTO FILE 'select-a.0';" | $HT_HOME/bin/ht shell --batch

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/dump-table.hql > dump.tsv

kill -9 `cat $PIDFILE`

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE >> rangeserver.output &

echo "USE '/'; DROP TABLE IF EXISTS LoadTest; CREATE TABLE LoadTest ( Field );" | $HT_HOME/bin/ht shell --batch

echo "USE '/'; LOAD DATA INFILE 'dump.tsv' INTO TABLE LoadTest;" | $HT_HOME/bin/ht shell --batch

echo "USE '/'; SELECT * FROM LoadTest INTO FILE 'select-b.0';" | $HT_HOME/bin/ht shell --batch

diff select-a.0 select-b.0

if [ $? != 0 ] ; then
    kill -9 `cat $PIDFILE`
    exit 1
fi

kill -9 `cat $PIDFILE`
exit 0
