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

VALGRIND_RANGESERVER=
VALGRIND_MASTER=

START_RANGESERVER="true"
START_MASTER="true"

CONFIG_OPT=

while [ "$1" != "${1##[-+]}" ]; do
    case $1 in
	'')    
	    echo $"$0: Usage: start-range-server.sh [local|hadoop|kosmos]"
	    exit 1;;
	--valgrind-range-server)
	    VALGRIND_RANGESERVER="valgrind -v --log-file=vg --leak-check=full --num-callers=20 "
	    shift
	    ;;
	--no-range-server)
	    START_RANGESERVER="false"
	    shift
	    ;;
	--config)
	    shift
	    CONFIG_OPT="--config=$1"
	    shift
	    ;;
	*)     
	    echo $"$0: Usage: start-range-server.sh [local|hadoop|kosmos]"
	    exit 1;;
    esac
done

if [ "$#" -eq 0 ]; then
    echo $"$0: Usage: start-range-server.sh [local|hadoop|kosmos]"
    exit 1
fi

START_COMMITLOG_BROKER=no
#if [ "$1" == "hadoop" ] ; then
#      START_COMMITLOG_BROKER=yes
#fi

RANGESERVER_OPTS=

#
# Start DfsBroker
#
PIDFILE=$HYPERTABLE_HOME/run/DfsBroker.$1.pid
LOGFILE=$HYPERTABLE_HOME/log/DfsBroker.$1.log

$HYPERTABLE_HOME/bin/serverup dfsbroker
if [ $? != 0 ] ; then

  if [ "$1" == "hadoop" ] ; then
      nohup $HYPERTABLE_HOME/bin/jrun --pidfile $PIDFILE org.hypertable.DfsBroker.hadoop.main $CONFIG_OPT --verbose 1>& $LOGFILE &
  elif [ "$1" == "kosmos" ] ; then
      nohup $HYPERTABLE_HOME/bin/kosmosBroker $CONFIG_OPT --pidfile=$PIDFILE --verbose 1>& $LOGFILE &   
  elif [ "$1" == "local" ] ; then
      nohup $HYPERTABLE_HOME/bin/localBroker $CONFIG_OPT --pidfile=$PIDFILE --verbose 1>& $LOGFILE &    
  else
      echo $"$0: Usage: start-range-server.sh [local|hadoop|kosmos]"      
      exit 1
  fi

  sleep 1
  $HYPERTABLE_HOME/bin/serverup dfsbroker
  if [ $? != 0 ] ; then
      echo -n "DfsBroker ($1) hasn't come up yet, trying again in 5 seconds ..."
      sleep 5
      echo ""
      $HYPERTABLE_HOME/bin/serverup dfsbroker
      if [ $? != 0 ] ; then
	  tail -100 $LOGFILE
	  echo "Problem statring DfsBroker ($1)";
	  exit 1
      fi
  fi
  echo "Successfully started DFSBroker ($1)"
fi

#
# Commit Log broker
#
$HYPERTABLE_HOME/bin/serverup --host=localhost --port=38031 dfsbroker
if [ "$START_COMMITLOG_BROKER" == "yes" ] ; then
      nohup $HYPERTABLE_HOME/bin/localBroker $CONFIG_OPT --listen-port=38031 --pidfile=$HYPERTABLE_HOME/run/CommitLog.DfsBroker.$1.pid --verbose 1>& $HYPERTABLE_HOME/log/CommitLog.DfsBroker.$1.log &
      RANGESERVER_OPTS="--log-broker=localhost:38031"

  sleep 1
  $HYPERTABLE_HOME/bin/serverup --host=localhost --port=38031 dfsbroker
  if [ $? != 0 ] ; then
      echo -n "DfsBroker (local) hasn't come up yet, trying again in 5 seconds ..."
      sleep 5
      echo ""
      $HYPERTABLE_HOME/bin/serverup --host=localhost --port=38031 dfsbroker
      if [ $? != 0 ] ; then
	  tail -100 $LOGFILE
	  echo "Problem statring Commit Log DfsBroker (local)";
	  exit 1
      fi
  fi
  echo "Successfully started Commit Log DFSBroker (local)"
fi


#
# Start Hypertable.RangeServer
#
if [ "$START_RANGESERVER" == "true" ] ; then

    PIDFILE=$HYPERTABLE_HOME/run/Hypertable.RangeServer.pid
    LOGFILE=$HYPERTABLE_HOME/log/Hypertable.RangeServer.log

    $HYPERTABLE_HOME/bin/serverup rangeserver
    if [ $? != 0 ] ; then
	nohup $VALGRIND_RANGESERVER $HYPERTABLE_HOME/bin/Hypertable.RangeServer $CONFIG_OPT $RANGESERVER_OPTS --pidfile=$PIDFILE --verbose 1>& $LOGFILE &
	sleep 1
	$HYPERTABLE_HOME/bin/serverup rangeserver
	if [ $? != 0 ] ; then
	    echo -n "Hypertable.RangeServer hasn't come up yet, trying again in 5 seconds ..."
	    sleep 5
	    echo ""
	    $HYPERTABLE_HOME/bin/serverup rangeserver
	    if [ $? != 0 ] ; then
		tail -100 $LOGFILE
		echo "Problem statring Hypertable.RangeServer";
		exit 1
	    fi
	fi
    fi
    echo "Successfully started Hypertable.RangeServer"
else
    exit 0
fi
