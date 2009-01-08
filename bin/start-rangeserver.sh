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

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)

# Make sure log and run directories exist
if [ ! -d $HYPERTABLE_HOME/run ] ; then
  mkdir $HYPERTABLE_HOME/run
fi
if [ ! -d $HYPERTABLE_HOME/log ] ; then
  mkdir $HYPERTABLE_HOME/log
fi

VALGRIND=

usage() {
  echo ""
  echo "usage: start-rangeserver.sh [OPTIONS] [<server-options>]"
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
      break
      ;;
  esac
done

PIDFILE=$HYPERTABLE_HOME/run/Hypertable.RangeServer.pid
LOGFILE=$HYPERTABLE_HOME/log/Hypertable.RangeServer.log

if type cronolog > /dev/null 2>&1; then
  LOGGER="| cronolog --link $LOGFILE \
    $HYPERTABLE_HOME/log/archive/%Y-%m/%d/Hypertable.RangeServer.log"
else
  LOGGER="> $LOGFILE"
fi

let RETRY_COUNT=0
$HYPERTABLE_HOME/bin/serverup --silent rangeserver
if [ $? != 0 ] ; then
  eval $VALGRIND $HYPERTABLE_HOME/bin/Hypertable.RangeServer \
    --pidfile=$PIDFILE --verbose "$@" '2>&1' $LOGGER &> /dev/null &

  $HYPERTABLE_HOME/bin/serverup --silent rangeserver
  while [ $? != 0 ] ; do
    let RETRY_COUNT=++RETRY_COUNT
    let REPORT=RETRY_COUNT%10
    if [ $RETRY_COUNT == 200 ] ; then
      echo "ERROR: Hypertable.RangeServer did not come up"
      exit 1
    elif [ $REPORT == 0 ] ; then
    echo "Waiting for Hypertable.RangeServer to come up ..."
    fi
    sleep 1
    $HYPERTABLE_HOME/bin/serverup --silent rangeserver
  done
  echo "Successfully started Hypertable.RangeServer"
else
  echo "WARNING: Hypertable.RangeServer already running."
fi

