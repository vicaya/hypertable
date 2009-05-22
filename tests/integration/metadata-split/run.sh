#!/bin/sh

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"20000000"}

$HT_HOME/bin/clean-database.sh

rm -rf $HT_HOME/hyperspace/* $HT_HOME/fs/*

$HT_HOME/bin/start-all-servers.sh --no-rangeserver --no-thriftbroker local

$HT_HOME/bin/Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.Lib.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.RangeServer.Range.SplitSize=10K \
    --Hypertable.RangeServer.Range.MetadataSplitSize=2K \
    --Hypertable.RangeServer.MaintenanceThreads=8 \
    --Hypertable.RangeServer.Maintenance.Interval=100 >& rangeserver.output &

sleep 2

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht_load_generator update --spec-file=data.spec --max-bytes=100M

sleep 10

fgrep ERROR rangeserver.output

if [ $? == 0 ] ; then
    exit 1
fi

exit 0
