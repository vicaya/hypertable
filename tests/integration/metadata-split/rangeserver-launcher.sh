#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
LAUNCHER_PIDFILE=$HT_HOME/run/Hypertable.RangeServerLauncher.pid
DUMP_METALOG=$HT_HOME/bin/dump_metalog
MY_IP=`$HT_HOME/bin/ht system_info --my-ip`
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

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.RangeServer.Range.SplitSize=25K \
    --Hypertable.RangeServer.CellStore.DefaultBlockSize=1K \
    --Hypertable.RangeServer.Range.MetadataSplitSize=3K \
    --Hypertable.RangeServer.MaintenanceThreads=8 \
    --Hypertable.RangeServer.Maintenance.Interval=100 $@

[ "$1" ] || exit # base run

echo ""
echo "!!!! CRASH ($@) !!!!"
echo ""

ulimit -c unlimited

$HT_HOME/bin/ht Hypertable.RangeServer --pidfile=$PIDFILE --verbose
