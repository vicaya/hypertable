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

ulimit -c unlimited

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


#
# Make sure log and run directories exist
#
if [ ! -d $HYPERTABLE_HOME/run ] ; then
  mkdir $HYPERTABLE_HOME/run
fi
if [ ! -d $HYPERTABLE_HOME/log ] ; then
  mkdir $HYPERTABLE_HOME/log
fi

cd $HYPERTABLE_HOME/run

VALGRIND=

usage() {
  echo ""
  echo "usage: start-dfsbroker.sh [OPTIONS] (local|hadoop|kfs) [<global-args>]"
  echo ""
  echo "OPTIONS:"
  echo "  --valgrind  run broker with valgrind"
  echo ""
}

while [ "$1" != "${1##[-+]}" ]; do
  case $1 in
    '')
      usage
      exit 1;;
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

PIDFILE=$HYPERTABLE_HOME/run/DfsBroker.$DFS.pid
LOGFILE=$HYPERTABLE_HOME/log/DfsBroker.$DFS.log

if type cronolog > /dev/null 2>&1; then
  LOGGER="| cronolog --link $LOGFILE \
    $HYPERTABLE_HOME/log/archive/%Y-%m/%d/DfsBroker.$DFS.log"
else
  LOGGER="> $LOGFILE"
fi

let RETRY_COUNT=0
$HYPERTABLE_HOME/bin/serverup --silent dfsbroker
if [ $? != 0 ] ; then

  if [ "$DFS" == "hadoop" ] ; then
    if [ "n$VALGRIND" != "n" ] ; then
      echo "ERROR: hadoop broker cannot be run with valgrind"
      exit 1
    fi
    eval $HYPERTABLE_HOME/bin/jrun --pidfile $PIDFILE \
      org.hypertable.DfsBroker.hadoop.main --verbose "$@" '2>&1' $LOGGER \
      &> /dev/null &
  elif [ "$DFS" == "kfs" ] ; then
    eval $VALGRIND $HYPERTABLE_HOME/bin/kosmosBroker \
      --pidfile=$PIDFILE --verbose "$@" '2>&1' $LOGGER &> /dev/null &
  elif [ "$DFS" == "local" ] ; then
    eval $VALGRIND $HYPERTABLE_HOME/bin/localBroker \
      --pidfile=$PIDFILE --verbose "$@" '2>&1' $LOGGER &> /dev/null &
  else
    usage
    exit 1
  fi

  $HYPERTABLE_HOME/bin/serverup --silent dfsbroker
  while [ $? != 0 ] ; do
    let RETRY_COUNT=++RETRY_COUNT
    let REPORT=RETRY_COUNT%3
    if [ $RETRY_COUNT == 10 ] ; then
      echo "ERROR: DfsBroker ($DFS) did not come up"
      exit 1
    elif [ $REPORT == 0 ] ; then
    echo "Waiting for DfsBroker ($DFS) to come up ..."
    fi
    sleep 1
    $HYPERTABLE_HOME/bin/serverup --silent dfsbroker
  done
  echo "Successfully started DFSBroker ($DFS)"
else
  echo "WARNING: DFSBroker already running."
fi

