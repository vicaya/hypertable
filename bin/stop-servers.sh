#!/usr/bin/env bash
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

FORCE="false"

STOP_DFSBROKER="true"
STOP_MASTER="true"
STOP_RANGESERVER="true"
STOP_THRIFTBROKER="true"
STOP_HYPERSPACE="true"
STOP_TESTCLIENT="true"
STOP_TESTDISPATCHER="true"

usage() {
  echo ""
  echo "usage: stop-servers.sh [OPTIONS]"
  echo ""
  echo "OPTIONS:"
  echo "  --force                 kill processes to ensure they're stopped"
  echo "  --no-dfsbroker          do not stop the DFS broker"
  echo "  --no-master             do not stop the Hypertable master"
  echo "  --no-rangeserver        do not stop the RangeServer"
  echo "  --no-hyperspace         do not stop the Hyperspace master"
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
    --force)
      FORCE="true"
      shift
      ;;
    --no-dfsbroker)
      STOP_DFSBROKER="false"
      shift
      ;;
    --no-master)
      STOP_MASTER="false"
      shift
      ;;
    --no-rangeserver)
      STOP_RANGESERVER="false"
      shift
      ;;
    --no-hyperspace)
      STOP_HYPERSPACE="false"
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
# Stop TestClient
#
if [ $STOP_TESTCLIENT == "true" ] ; then
  stop_server testclient
fi

#
# Stop TestDispatcher
#
if [ $STOP_TESTDISPATCHER == "true" ] ; then
  stop_server testdispatcher
fi

#
# Stop Thriftbroker 
#
if [ $STOP_THRIFTBROKER == "true" ] ; then
  stop_server thriftbroker
fi

#
# Stop Master
#
if [ $STOP_MASTER == "true" ] ; then
  echo 'shutdown;quit;' | $HYPERTABLE_HOME/bin/ht master_client --batch
  # wait for master shutdown
  wait_for_server_shutdown master "master" "$@"
  if [ $FORCE == "true" ] ; then
      stop_server master
  fi
fi

#
# Stop RangeServer
#
if [ $STOP_RANGESERVER == "true" ] ; then
  echo "Sending shutdown command"
  echo 'shutdown;quit' | $HYPERTABLE_HOME/bin/ht rsclient --batch --no-hyperspace
  # wait for rangeserver shutdown
  wait_for_server_shutdown rangeserver "range server" "$@"
  if [ $FORCE == "true" ] ; then
      stop_server rangeserver
  fi
fi


#
# Stop DFSBroker
#
if [ $STOP_DFSBROKER == "true" ] ; then
  stop_server dfsbroker
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

# wait for master shutdown
if [ $STOP_MASTER == "true" ] ; then
  wait_for_server_shutdown master "hypertable master" "$@" &
fi

# wait for hyperspace shutdown
if [ $STOP_HYPERSPACE == "true" ] ; then
    wait_for_server_shutdown hyperspace "hyperspace" "$@" &
fi

wait
