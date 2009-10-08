#!/bin/bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
LAUNCHER_PIDFILE=$HT_HOME/run/Hypertable.RangeServerLauncher.pid
DUMP_METALOG=$HT_HOME/bin/dump_metalog
MY_IP=`$HT_HOME/bin/system_info --my-ip`
RS_PORT=38060
METALOG="/hypertable/servers/${MY_IP}_${RS_PORT}/log/range_txn/0.log"
RANGE_SIZE=${RANGE_SIZE:-"7M"}

# Kill launcher if running & store pid of this launcher
if [ -f $LAUNCHER_PIDFILE ]; then
  kill -9 `cat $LAUNCHER_PIDFILE`
  rm -f $LAUNCHER_PIDFILE
fi
echo "$$" > $LAUNCHER_PIDFILE

# Kill RangeServer if running
if [ -f $PIDFILE ]; then
  kill -9 `cat $PIDFILE`
  rm -f $PIDFILE
fi

# Dumping cores slows things down unnecessarily for normal test runs
ulimit -c 0

$HT_HOME/bin/Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.RangeServer.UpdateDelay=100 \
    --Hypertable.RangeServer.Range.SplitSize=$RANGE_SIZE $@

[ "$1" ] || exit # base run

echo ""
echo "!!!! CRASH ($@) !!!!"
echo ""

echo "RSML entries:"
$DUMP_METALOG $METALOG
echo "Range states:"
$DUMP_METALOG -s $METALOG

$HT_HOME/bin/Hypertable.RangeServer --pidfile=$PIDFILE --verbose
