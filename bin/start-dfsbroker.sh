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

echo "DFS broker: available file descriptors: `ulimit -n`"

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

usage() {
  echo ""
  echo "usage: start-dfsbroker.sh [OPTIONS] (local|hadoop|kfs|ceph) [<global-args>]"
  echo ""
  echo "OPTIONS:"
  echo "  --valgrind  run broker with valgrind"
  echo ""
}

while [ "$1" != "${1##[-+]}" ]; do
  case $1 in
    --valgrind)
      VALGRIND="valgrind -v --log-file=vg --leak-check=full --num-callers=20 "
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

set_start_vars DfsBroker.$DFS
check_pidfile $pidfile && exit 0

check_server dfsbroker "$@"
if [ $? != 0 ] ; then
  if [ "$DFS" == "hadoop" ] ; then
    if [ "n$VALGRIND" != "n" ] ; then
      echo "ERROR: hadoop broker cannot be run with valgrind"
      exit 1
    fi
    exec_server jrun org.hypertable.DfsBroker.hadoop.main --verbose "$@"
  elif [ "$DFS" == "kfs" ] ; then
    exec_server kosmosBroker --verbose "$@"
  elif [ "$DFS" == "ceph" ] ; then
    exec_server cephBroker --verbose "$@"
  elif [ "$DFS" == "local" ] ; then
    exec_server localBroker --verbose "$@"
  else
    usage
    exit 1
  fi

  wait_for_server_up dfsbroker "DFS Broker ($DFS)" "$@"
else
  echo "WARNING: DFSBroker already running."
fi
