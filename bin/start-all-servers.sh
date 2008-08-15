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
pushd . >& /dev/null
HYPERTABLE_HOME=`dirname "$this"`/..
cd $HYPERTABLE_HOME
export HYPERTABLE_HOME=`pwd`
popd >& /dev/null


#
# Make sure log and run directories exist
#
if [ ! -d $HYPERTABLE_HOME/run ] ; then
  mkdir $HYPERTABLE_HOME/run
fi
if [ ! -d $HYPERTABLE_HOME/log ] ; then
  mkdir $HYPERTABLE_HOME/log
fi


RANGESERVER_OPTS=
MASTER_OPTS=
HYPERSPACE_OPTS=

START_RANGESERVER="true"
START_MASTER="true"

usage() {
  echo ""
  echo "usage: start-all-servers.sh [OPTIONS] <dfs-choice> [<global-options>]"
  echo ""
  echo "OPTIONS:"
  echo "  --valgrind-rangeserver  run Hypertable.RangeServer with valgrind"
  echo "  --valgrind-master     run Hypertable.Master with valgrind"
  echo "  --valgrind-hyperspace   run Hyperspace.Master with valgrind"
  echo "  --no-rangeserver    do not launch the range server"
  echo "  --no-master       do not launch the Hypertable master"
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
    --no-rangeserver)
      START_RANGESERVER="false"
      shift
      ;;
    --no-master)
      START_MASTER="false"
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
$HYPERTABLE_HOME/bin/start-dfsbroker.sh $DFS $@
if [ $? != 0 ] ; then
  echo "Error starting DfsBroker ($DFS)"
fi


#
# Start Hyperspace
#
$HYPERTABLE_HOME/bin/start-hyperspace.sh $HYPERSPACE_OPTS $@
if [ $? != 0 ] ; then
  echo "Error starting Hyperspace"
fi


#
# Start Hypertable.Master
#
if [ $START_MASTER == "true" ] ; then
  $HYPERTABLE_HOME/bin/start-master.sh $MASTER_OPTS $@
  if [ $? != 0 ] ; then
    echo "Error starting Hypertable.Master"
  fi
fi


#
# Start Hypertable.RangeServer
#
if [ $START_RANGESERVER == "true" ] ; then
  $HYPERTABLE_HOME/bin/start-rangeserver.sh $RANGESERVER_OPTS $@
  if [ $? != 0 ] ; then
    echo "Error starting Hypertable.RangeServer"
  fi
fi

