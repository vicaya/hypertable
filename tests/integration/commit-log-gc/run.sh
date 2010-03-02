#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME
SCRIPT_DIR=`dirname $0`
NUM_POLLS=${NUM_POLLS:-"10"}
MY_IP=`$HT_HOME/bin/system_info --my-ip`
WRITE_TOTAL=${WRITE_TOTAL:-"30000000"}
WRITE_SIZE=${WRITE_SIZE:-"500000"}

. $HT_HOME/bin/ht-env.sh

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
   --Hypertable.RangeServer.CommitLog.RollLimit 1M \
   --Hypertable.RangeServer.CommitLog.Compressor none \
   --Hypertable.RangeServer.Maintenance.Interval 100 \
   --Hypertable.RangeServer.Range.SplitSize=1M

$HT_HOME/bin/hypertable --no-prompt < $SCRIPT_DIR/create-table.hql

for ((i=1; i<10; i++)) ; do

    $HT_HOME/bin/ht_load_generator update \
        --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=10K \
        --rowkey.component.0.type=integer \
        --rowkey.component.0.format="%010lld" \
        --rowkey.component.0.min=${i}10000 \
        --rowkey.component.0.max=${i}90000 \
        --Field.value.size=10 \
        --max-bytes=$WRITE_SIZE

    sleep 0.1

    $HT_HOME/bin/ht_load_generator update \
        --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=10K \
        --rowkey.component.0.type=integer \
        --rowkey.component.0.order=random \
        --rowkey.component.0.format="%010lld" \
        --rowkey.component.0.min=${i}00000 \
        --rowkey.component.0.max=${i}99999 \
        --Field.value.size=10 \
        --max-bytes=$WRITE_SIZE

done

while [ $NUM_POLLS -gt 0 ]; do
  sleep 2
  n=`ls $HT_HOME/fs/local/hypertable/servers/${MY_IP}_38060/log/user | wc -l`
  if [ $n -le 13 ] ; then
      NUM_POLLS=3
      while [ $NUM_POLLS -gt 0 ]; do
          n=`ls $HT_HOME/fs/local/hypertable/servers/${MY_IP}_38060/log | wc -l`
          [ $n -le 13 ] && exit 0
          sleep 2
          NUM_POLLS=$((NUM_POLLS - 1))
      done
      echo "ERROR: split logs not cleaned up:"
      ls $HT_HOME/fs/local/hypertable/servers/${MY_IP}_38060/log
      exit 1
  fi
  NUM_POLLS=$((NUM_POLLS - 1))
done
echo "ERROR: split logs not cleaned up:"
ls -l $HT_HOME/fs/local/hypertable/servers/${MY_IP}_38060/log/user
exit 1
