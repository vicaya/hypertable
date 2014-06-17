#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker --clear

$HT_HOME/bin/Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.RangeServer.Range.SplitSize=10000000 \
    --Hypertable.RangeServer.AccessGroup.GarbageThreshold.Percentage=20 \
    --Hypertable.RangeServer.Maintenance.Interval=100 \
    --Hypertable.RangeServer.Timer.Interval=100 \
    --Hypertable.RangeServer.AccessGroup.MaxMemory=250000 \
    $@ > rangeserver.output 2>&1 &

echo "use '/'; create table LoadTest ( Field ) MAX_VERSIONS=1;" | $HT_SHELL --batch

for ((i=1; i<10; i++)) ; do

  $HT_HOME/bin/ht ht_load_generator update \
    --row-seed=1 \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.order=random \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.max=10000 \
    --Field.value.size=10000 \
    --max-bytes=500000

  perl -e "select(undef,undef,undef,0.2);"

done

kill -9 `cat $PIDFILE`

n=`fgrep "Switching from minor to major" rangeserver.output | wc -l`
if [ $n -eq 0 ] ; then
  echo "RangeServer did not switch to major compaction for gc purposes"
  exit 1
fi

exit 0


