#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
NUM_POLLS=${NUM_POLLS:-"10"}
MY_IP=`$HT_HOME/bin/system_info --my-ip`
WRITE_SIZE=${WRITE_SIZE:-"20000000"}

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
   --Hypertable.RangeServer.CommitLog.Compressor none \
   --Hypertable.RangeServer.Maintenance.Interval 10 \
   --Hypertable.RangeServer.Range.SplitSize=300K

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=50 \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%010lld" \
    --rowkey.component.0.min=${i}10000 \
    --rowkey.component.0.max=${i}90000 \
    --Field.value.size=10000 \
    --max-bytes=$WRITE_SIZE

sleep 5

$HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/dump-table.hql > keys.output

diff keys.output ${SCRIPT_DIR}/keys.golden
