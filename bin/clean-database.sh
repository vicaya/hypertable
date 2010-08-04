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

confirm=y

if tty > /dev/null && [ $# == 1 ]; then
  case $1 in
    -i|--interactive)
      echo "Are you sure you want to clear the database (on default ports)?"
      echo "Will proceed in 10 seconds, (Ctrl-C to quit)"
      shift
      sleep 10
      ;;
  esac
fi

# Stop servers other than dfsbroker
stop_server thriftbroker rangeserver master hyperspace
sleep 1
wait_for_server_shutdown thriftbroker "thrift broker" "$@" &
wait_for_server_shutdown rangeserver "range server" "$@" &
wait_for_server_shutdown master "hypertable master" "$@" &
wait_for_server_shutdown hyperspace "hyperspace" "$@" &

case $confirm in
  y|Y)
    #
    # Clear state
    #
    check_server dfsbroker "$@"
    if [ $? != 0 ] ; then
      echo "ERROR: DfsBroker not running, database not cleaned"
      # remove local stuff anyway.
      rm -rf $HYPERTABLE_HOME/hyperspace/* $HYPERTABLE_HOME/fs/* $HYPERTABLE_HOME/run/rsml_backup/*
      exit 1
    fi

    TOPLEVEL="/"`$HYPERTABLE_HOME/bin/get_property $@ Hypertable.Directory | tr -d "/"`

    $HYPERTABLE_HOME/bin/dfsclient --timeout 60000 --eval "rmdir $TOPLEVEL/servers" "$@"
    $HYPERTABLE_HOME/bin/dfsclient --timeout 60000 --eval "rmdir $TOPLEVEL/tables" "$@"
    echo "Removed $TOPLEVEL/servers in DFS"
    echo "Removed $TOPLEVEL/tables in DFS"
    /bin/rm -rf $HYPERTABLE_HOME/hyperspace/*
    /bin/rm -rf $HYPERTABLE_HOME/run/rsml_backup/*
    /bin/rm -rf $HYPERTABLE_HOME/run/location
    /bin/rm -rf $HYPERTABLE_HOME/run/last-dfs
    echo "Cleared hyperspace"
    /bin/rm -rf $HYPERTABLE_HOME/run/monitoring/*
    echo "Cleared monitoring data"
    ;;
  *) echo "Database not cleared";;
esac

#
# Stop dfsbroker
#
stop_server dfsbroker
sleep 1
wait_for_server_shutdown dfsbroker "DFS broker" "$@" &
wait
