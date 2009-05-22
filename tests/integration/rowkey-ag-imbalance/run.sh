#!/bin/sh

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"20000000"}

$HT_HOME/bin/clean-database.sh

rm -rf $HT_HOME/hyperspace/* $HT_HOME/fs/*

$HT_HOME/bin/start-all-servers.sh --no-rangeserver local

$HT_HOME/bin/Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.RangeServer.Range.SplitSize=2000000 \
    --Hypertable.RangeServer.Maintenance.Interval=100 >& rangeserver.output &

sleep 2

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht_load_generator update --spec-file=foo.spec --max-bytes=2200000

sleep 2

$HT_HOME/bin/ht_load_generator update --spec-file=bar.spec --max-bytes=40000000 \
    --Hypertable.Lib.Mutator.FlushDelay=100

sleep 1

fgrep ERROR rangeserver.output

if [ $? == 0 ] ; then
    exit 1
fi

exit 0
