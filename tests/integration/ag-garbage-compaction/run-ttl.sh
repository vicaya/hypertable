#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
TTL=10

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker --clear

$HT_HOME/bin/Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.RangeServer.Range.SplitSize=10000000 \
    --Hypertable.RangeServer.AccessGroup.GarbageThreshold.Percentage=20 \
    --Hypertable.RangeServer.Maintenance.Interval=100 \
    --Hypertable.RangeServer.Timer.Interval=100 \
    --Hypertable.RangeServer.AccessGroup.MaxMemory=250000 \
    $@ > rangeserver.output 2>&1 &

date
echo "create table MAX_VERSIONS=1 LoadTest ( Field TTL=$TTL);" | $HT_SHELL --batch
start_time=`date "+%s"`

date
$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=150 \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=500000 \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.order=random \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.max=10000 \
    --Field.value.size=10000 \
    --rowkey.seed=1 \
    --max-bytes=5000000
date

finish_time=`date "+%s"`

echo "select Files from METADATA where ROW =^ '1:' REVS=1;" | $HT_SHELL --batch

let elapsed_time=$finish_time-$start_time
let wait_time=$TTL*3

if [ $elapsed_time -ge $wait_time ] ; then
  let sleep_time=$wait_time
else
  let sleep_time=$wait_time-$elapsed_time
fi

echo "Sleeping for $sleep_time seconds to give GC compaction a chance to run..."
sleep $sleep_time

# Make sure cell stores have been removed
lines=`echo "select Files from METADATA where ROW =^ '1:' REVS=1;" | $HT_SHELL --batch | fgrep "/cs" | wc -l`
n=`echo $lines | tr -d " "`
if [ $n != "0" ] ; then
  echo "RangeServer did not switch to major compaction for gc purposes ($n)"
  exit 1
fi

# Make sure no more data exists in table
lines=`echo "select * from LoadTest;" | $HT_SHELL --batch | wc -l`
n=`echo $lines | tr -d " "`
if [ $n != "0" ] ; then
  echo "Data ($n cells) still remaining in LoadTest"
  exit 1
fi

exit 0


