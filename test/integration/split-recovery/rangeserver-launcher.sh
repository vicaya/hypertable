#!/bin/sh

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
DUMP_METALOG=$HT_HOME/bin/dump_metalog
MY_IP=`$HT_HOME/bin/system_info --my-ip`
RS_PORT=38060
METALOG="/hypertable/servers/${MY_IP}_${RS_PORT}/log/range_txn/0.log"
RANGE_SIZE=${RANGE_SIZE:-"10M"}

$HT_HOME/bin/Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.RangeServer.Range.MaxBytes=$RANGE_SIZE $@

[ "$1" ] || exit # base run

echo ""
echo "!!!! CRASH ($@) !!!!"
echo ""

echo "RSML entries:"
$DUMP_METALOG $METALOG
echo "Range states:"
$DUMP_METALOG -s $METALOG

$HT_HOME/bin/Hypertable.RangeServer --pidfile=$PIDFILE --verbose
