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

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"


#
# The installation directory
#
HYPERTABLE_HOME=`dirname "$this"`/..
cd $HYPERTABLE_HOME
export HYPERTABLE_HOME=`pwd`
cd $HYPERTABLE_HOME/run

stop_server() {
  for pidfile in $@; do
    if [ -f $pidfile ] ; then
      kill -9 `cat $pidfile`
      rm $pidfile
    fi
  done
}

wait_for_server_shutdown() {
  server=$1
  serverdesc=$2
  $HYPERTABLE_HOME/bin/serverup --silent $server
  while [ $? == 0 ] ; do
    sleep 2
    echo "Waiting for $serverdesc to shutdown ..."
    $HYPERTABLE_HOME/bin/serverup --silent $server
  done
}

stop_server $HYPERTABLE_HOME/run/ThriftBroker.pid
stop_server $HYPERTABLE_HOME/run/DfsBroker.*.pid
stop_server $HYPERTABLE_HOME/run/Hypertable.RangeServer.pid
stop_server $HYPERTABLE_HOME/run/Hypertable.Master.pid
stop_server $HYPERTABLE_HOME/run/Hyperspace.pid
sleep 2
wait_for_server_shutdown thriftbroker "thrift broker"
wait_for_server_shutdown dfsbroker "DFS broker"
wait_for_server_shutdown rangeserver "range server"
wait_for_server_shutdown master "hypertable master"
wait_for_server_shutdown hyperspace "hyperspace"
