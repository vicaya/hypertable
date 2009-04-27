#!/bin/bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
NUM_POLLS=${NUM_POLLS:-"10"}
MY_IP=`$HT_HOME/bin/system_info --my-ip`
WRITE_TOTAL=${WRITE_TOTAL:-"50000000"}

$HT_HOME/bin/clean-database.sh

$HT_HOME/bin/start-all-servers.sh local \
   --Hypertable.RangeServer.CommitLog.RollLimit 1M \
   --Hypertable.RangeServer.CommitLog.Compressor none \
   --Hypertable.RangeServer.Maintenance.Interval 1000 \
   --Hypertable.RangeServer.Range.MaxBytes=2M

$HT_HOME/bin/hypertable --Hypertable.Lib.Mutator.FlushDelay=100 --no-prompt < $SCRIPT_DIR/create-table.hql

echo "================================="
echo "Two Maintenence Thread WRITE test"
echo "================================="
$HT_HOME/bin/random_write_test $WRITE_TOTAL # creates about 50 fragments

while [ $NUM_POLLS -gt 0 ]; do
  sleep 2
  n=`ls $HT_HOME/fs/local/hypertable/servers/${MY_IP}_38060/log/user | wc -l`
  if [ $n -lt 5 ] ; then
      NUM_POLLS=3
      while [ $NUM_POLLS -gt 0 ]; do
          n=`ls $HT_HOME/fs/local/hypertable/servers/${MY_IP}_38060/log | wc -l`
          [ $n -lt 6 ] && exit 0
          sleep 2
          NUM_POLLS=$((NUM_POLLS - 1))
      done
      echo "ERROR: split logs not cleaned up:"
      ls $HT_HOME/fs/local/hypertable/servers/${MY_IP}_38060/log
      exit 1
  fi
  NUM_POLLS=$((NUM_POLLS - 1))
done
exit 1
