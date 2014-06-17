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

dfs_conflict_error() {
    OLD_DFS=$1
    shift
    NEW_DFS=$1
    echo ""
    echo "ERROR: DFS conflict"
    echo ""
    echo "You are trying to run Hypertable with the '$NEW_DFS' broker"
    echo "on a system that was previously run with the '$OLD_DFS' broker."
    echo ""
    if [ "$OLD_DFS" == "local" ] ; then
        echo "Run the following command to remove the previous database,"
        echo "and all of its associated state, before launching with the"
        echo "'$NEW_DFS' broker:"
        echo ""
        echo "$HYPERTABLE_HOME/bin/stop-servers.sh ; $HYPERTABLE_HOME/bin/start-dfsbroker.sh $OLD_DFS ; $HYPERTABLE_HOME/bin/clean-database.sh"
        echo ""
    else
        echo "To remove the previous database, and all it's associated state,"
        echo "in order to launch with the '$NEW_DFS' broker, start the system"
        echo "on the old DFS and then clean the database.  For example, with"
        echo "Capistrano:"
        echo ""
        echo "cap stop ; cap -S dfs=$OLD_DFS cleandb"
        echo ""
    fi
    echo "Alternatively, you can manually purge the database state by issuing"
    echo "the following command on each Master and Hyperspace replica machine:"
    echo ""
    echo "rm -rf $HYPERTABLE_HOME/hyperspace/* $HYPERTABLE_HOME/fs/* $HYPERTABLE_HOME/run/rsml_backup/* $HYPERTABLE_HOME/run/last-dfs"
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

if [ -e $HYPERTABLE_HOME/run/last-dfs ] ; then
    LAST_DFS=`cat $HYPERTABLE_HOME/run/last-dfs`
    if [ "$DFS" != "$LAST_DFS" ] ; then
        dfs_conflict_error $LAST_DFS $DFS
        exit 1
    fi
else
    # record last DFS
    echo $DFS > $HYPERTABLE_HOME/run/last-dfs
fi

set_start_vars DfsBroker.$DFS
check_pidfile $pidfile && exit 0

check_server "$@" dfsbroker 
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
