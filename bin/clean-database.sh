#!/bin/bash
#
# Copyright 2008 Doug Judd (Zvents, Inc.)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)

stop_server() {
  for pidfile in $@; do
    if [ -f $pidfile ] ; then
      pid=`cat $pidfile`
      echo "Killing `basename $pidfile` $pid"
      kill -9 $pid
      rm $pidfile
    fi
  done
}

wait_for_server_shutdown() {
  server=$1
  serverdesc=$2
  $HYPERTABLE_HOME/bin/serverup --silent $server
  while [ $? == 0 ] ; do
    echo "Waiting for $serverdesc to shutdown ..."
    sleep 1
    $HYPERTABLE_HOME/bin/serverup --silent $server
  done
  echo "Shutdown $serverdesc complete"
}

# Stop servers other than dfsbroker
stop_server $HYPERTABLE_HOME/run/ThriftBroker.pid
stop_server $HYPERTABLE_HOME/run/Hypertable.RangeServer.pid
stop_server $HYPERTABLE_HOME/run/Hypertable.Master.pid
stop_server $HYPERTABLE_HOME/run/Hyperspace.pid
sleep 1
wait_for_server_shutdown thriftbroker "thrift broker"
wait_for_server_shutdown rangeserver "range server"
wait_for_server_shutdown master "hypertable master"
wait_for_server_shutdown hyperspace "hyperspace"

#
# Clear state
#
$HYPERTABLE_HOME/bin/serverup --silent dfsbroker
if [ $? != 0 ] ; then
  echo "ERROR: DfsBroker not running, database not cleaned"
  exit 1
fi

$HYPERTABLE_HOME/bin/dfsclient --eval "rmdir /hypertable/servers"
$HYPERTABLE_HOME/bin/dfsclient --eval "rmdir /hypertable/tables"
rm -rf $HYPERTABLE_HOME/hyperspace/* $HYPERTABLE_HOME/fs/*
echo "Removed /hypertable/servers in DFS"
echo "Removed /hypertable/tables in DFS"
echo "Cleared hyperspace"

#
# Stop dfsbroker
#
stop_server $HYPERTABLE_HOME/run/DfsBroker.*.pid
sleep 1
wait_for_server_shutdown dfsbroker "DFS broker"
