#!/bin/bash
#
# Copyright 2007 Doug Judd (Zvents, Inc.)
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
pushd . >& /dev/null
HYPERTABLE_HOME=`dirname "$this"`/..
cd $HYPERTABLE_HOME
export HYPERTABLE_HOME=`pwd`
popd >& /dev/null


$HYPERTABLE_HOME/bin/serverup --silent dfsbroker
if [ $? != 0 ] ; then
  echo "ERROR: DfsBroker not running, database not cleaned"
  exit 1
fi

#
# Stop rangeserver
#
pidfile="$HYPERTABLE_HOME/run/Hypertable.RangeServer.pid"
if [ -f $pidfile ] ; then
  kill -9 `cat $pidfile`
  rm $pidfile
fi

#
# Stop master
#
pidfile="$HYPERTABLE_HOME/run/Hypertable.Master.pid"
if [ -f $pidfile ] ; then
  kill -9 `cat $pidfile`
  rm $pidfile
fi

#
# Stop hyperspace
#
pidfile="$HYPERTABLE_HOME/run/Hyperspace.pid"
if [ -f $pidfile ] ; then
  kill -9 `cat $pidfile`
  rm $pidfile
fi

#
# Clear state
#
$HYPERTABLE_HOME/bin/dfsclient --eval "rmdir /hypertable/servers"
$HYPERTABLE_HOME/bin/dfsclient --eval "rmdir /hypertable/tables"
rm -rf $HYPERTABLE_HOME/hyperspace/* $HYPERTABLE_HOME/fs/*

#
# Stop dfsbroker
#
for pidfile in $HYPERTABLE_HOME/run/DfsBroker.*.pid ; do
  if [ "$pidfile" != "$HYPERTABLE_HOME/run/DfsBroker.*.pid" ] ; then
    kill `cat $pidfile`
    rm $pidfile
  fi
done

sleep 2

#
# Wait for Dfs broker to shutdown
#
$HYPERTABLE_HOME/bin/serverup --silent dfsbroker
while [ $? == 0 ] ; do
  sleep 2
  echo "Waiting for DFS Broker to shutdown ..."
  $HYPERTABLE_HOME/bin/serverup --silent dfsbroker
done

#
# Wait for RangeServer to shutdown
#
$HYPERTABLE_HOME/bin/serverup --silent rangeserver
while [ $? == 0 ] ; do
  sleep 2
  echo "Waiting for RangeServer to shutdown ..."
  $HYPERTABLE_HOME/bin/serverup --silent rangeserver
done

#
# Wait for Master to shutdown
#
$HYPERTABLE_HOME/bin/serverup --silent master
while [ $? == 0 ] ; do
  sleep 2
  echo "Waiting for Hypertable.Master to shutdown ..."
  $HYPERTABLE_HOME/bin/serverup --silent master
done

#
# Wait for Hyperspace to shutdown
#
$HYPERTABLE_HOME/bin/serverup --silent hyperspace
while [ $? == 0 ] ; do
  sleep 2
  echo "Waiting for Hyperspace to shutdown ..."
  $HYPERTABLE_HOME/bin/serverup --silent hyperspace
done
