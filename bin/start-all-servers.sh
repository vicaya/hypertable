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

RANGESERVER_OPTS=
MASTER_OPTS=
HYPERSPACE_OPTS=

START_RANGESERVER="true"
START_MASTER="true"
START_THRIFTBROKER="true"

usage() {
  echo ""
  echo "usage: start-all-servers.sh [OPTIONS] <dfs-choice> [<global-options>]"
  echo ""
  echo "OPTIONS:"
  echo "  --valgrind-hyperspace   run Hyperspace.Master with valgrind"
  echo "  --valgrind-master       run Hypertable.Master with valgrind"
  echo "  --valgrind-rangeserver  run Hypertable.RangeServer with valgrind"
  echo "  --valgrind-thriftbroker run ThriftBroker with valgrind"
  echo "  --no-rangeserver        do not launch the range server"
  echo "  --no-master             do not launch the Hypertable master"
  echo "  --no-thriftbroker       do not launch the ThriftBroker"
  echo ""
  echo "DFS choices: kfs, hadoop, local"
  echo ""
}


while [ "$1" != "${1##[-+]}" ]; do
  case $1 in
    '')
      usage
      exit 1;;
    --valgrind-rangeserver)
      RANGESERVER_OPTS="--valgrind "
      shift
      ;;
    --valgrind-master)
      MASTER_OPTS="--valgrind "
      shift
      ;;
    --valgrind-hyperspace)
      HYPERSPACE_OPTS="--valgrind "
      shift
      ;;
    --valgrind-thriftbroker)
      THRIFTBROKER_OPTS="--valgrind "
      shift
      ;;
    --no-rangeserver)
      START_RANGESERVER="false"
      shift
      ;;
    --no-master)
      START_MASTER="false"
      shift
      ;;
    --no-thriftbroker)
      START_THRIFTBROKER="false"
      shift
      ;;
    *)
      usage
      exit 1;;
  esac
done

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

DFS=$1
shift

#
# Start DfsBroker
#
$HYPERTABLE_HOME/bin/start-dfsbroker.sh $DFS $@ &

#
# Start Hyperspace
#
$HYPERTABLE_HOME/bin/start-hyperspace.sh $HYPERSPACE_OPTS $@ &

wait

#
# Start Hypertable.Master
#
if [ $START_MASTER == "true" ] ; then
  $HYPERTABLE_HOME/bin/start-master.sh $MASTER_OPTS $@
fi

#
# Start Hypertable.RangeServer
#
if [ $START_RANGESERVER == "true" ] ; then
  $HYPERTABLE_HOME/bin/start-rangeserver.sh $RANGESERVER_OPTS $@
fi

#
# Start ThriftBroker (optional)
#
if [ $START_THRIFTBROKER == "true" ] ; then
  if [ -f $HYPERTABLE_HOME/bin/ThriftBroker ] ; then
    $HYPERTABLE_HOME/bin/start-thriftbroker.sh $THRIFTBROKER_OPTS $@
  fi
fi
