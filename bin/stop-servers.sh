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
. $HYPERTABLE_HOME/bin/ht-env.sh

STOP_DFSBROKER="true"
STOP_HYPERSPACE="true"
STOP_MASTER="true"
STOP_THRIFTBROKER="true"

usage() {
  echo ""
  echo "usage: stop-servers.sh [OPTIONS]"
  echo ""
  echo "OPTIONS:"
  echo "  --no-dfsbroker          do not stop the DFS broker"
  echo "  --no-hyperspace         do not stop the Hyperspace master"
  echo "  --no-master             do not stop the Hypertable master"
  echo "  --no-thriftbroker       do not stop the ThriftBroker"
  echo ""
  echo "DFS choices: kfs, hadoop, local"
  echo ""
}

while [ "$1" != "${1##[-+]}" ]; do
  case $1 in
    '')
      usage
      exit 1;;
    --no-dfsbroker)
      STOP_DFSBROKER="false"
      shift
      ;;
    --no-hyperspace)
      STOP_HYPERSPACE="false"
      shift
      ;;
    --no-master)
      STOP_MASTER="false"
      shift
      ;;
    --no-thriftbroker)
      STOP_THRIFTBROKER="false"
      shift
      ;;
    *)
      usage
      exit 1;;
  esac
done



#
# Stop RangeServer
#
echo "Sending shutdown command"
echo 'shutdown;quit' | $HYPERTABLE_HOME/bin/ht_rsclient --batch --no-hyperspace

#
# Stop Thriftbroker 
#
if [ $STOP_THRIFTBROKER == "true" ] ; then
  stop_server thriftbroker
fi

#
# Stop DFSBroker
#
if [ $STOP_DFSBROKER == "true" ] ; then
  stop_server dfsbroker
fi

#
# Stop Master
#
if [ $STOP_MASTER == "true" ] ; then
  stop_server master 
fi

#
# Stop Hyperspace
#
if [ $STOP_HYPERSPACE == "true" ] ; then
  stop_server hyperspace 
fi

sleep 1
# wait for thriftbroker shutdown
if [ $STOP_THRIFTBROKER == "true" ] ; then
  wait_for_server_shutdown thriftbroker "thrift broker" "$@" &
fi
# wait for dfsbroker shutdown
if [ $STOP_DFSBROKER == "true" ] ; then
  wait_for_server_shutdown dfsbroker "DFS broker" "$@" &
fi

# wait for rangeserver shutdown
wait_for_server_shutdown rangeserver "range server" "$@" &

# wait for master shutdown
if [ $STOP_MASTER == "true" ] ; then
  wait_for_server_shutdown master "hypertable master" "$@" &
fi

# wait for hyperspace shutdown
if [ $STOP_HYPERSPACE == "true" ] ; then
    wait_for_server_shutdown hyperspace "hyperspace" "$@" &
fi

wait
