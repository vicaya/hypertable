#!/bin/sh

HYPERTABLE_HOME=~/hypertable/current

$HYPERTABLE_HOME/bin/Hypertable.RangeServer --config=hypertable.cfg --verbose $@ >> rangeserver.output

echo "" >> rangeserver.output
echo "!!!! CRASH ($@) !!!!" >> rangeserver.output
echo "" >> rangeserver.output

sleep 2

PIDFILE=$HYPERTABLE_HOME/run/Hypertable.RangeServer.pid

$HYPERTABLE_HOME/bin/Hypertable.RangeServer --pidfile=$PIDFILE --config=hypertable.cfg --verbose >> rangeserver.output
