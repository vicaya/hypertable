#!/bin/sh

HYPERTABLE_HOME=~/hypertable/current

$HYPERTABLE_HOME/bin/Hypertable.RangeServer --verbose --Hypertable.RangeServer.Range.MaxBytes=20000000 $@ >> rangeserver.output

echo "" >> rangeserver.output
echo "!!!! CRASH ($@) !!!!" >> rangeserver.output
echo "" >> rangeserver.output

sleep 2

PIDFILE=$HYPERTABLE_HOME/run/Hypertable.RangeServer.pid

$HYPERTABLE_HOME/bin/Hypertable.RangeServer --pidfile=$PIDFILE --verbose >> rangeserver.output
