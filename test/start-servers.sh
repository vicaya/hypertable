#!/bin/sh
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

#
# Make sure log and run directories exist
#
if [ ! -d $HYPERTABLE_HOME/run ] ; then
    mkdir $HYPERTABLE_HOME/run
fi
if [ ! -d $HYPERTABLE_HOME/log ] ; then
    mkdir $HYPERTABLE_HOME/log
fi

VALGRIND=

while [ "$1" != "${1##[-+]}" ]; do
    case $1 in
	'')    
	    echo $"$0: Usage: stat-servers.sh [--initialize] [local|hadoop|kosmos]"
	    exit 1;;
	--initialize)
	    DO_INITIALIZATION="true"
	    shift
	    ;;
	--valgrind)
	    VALGRIND="valgrind -v --log-file=vg "
	    shift
	    ;;
	*)     
	    echo $"$0: Usage: stat-servers.sh [--initialize] [local|hadoop|kosmos]"
	    exit 1;;
    esac
done

if [ "$#" -eq 0 ]; then
    echo $"$0: Usage: stat-servers.sh [--initialize] [local|hadoop|kosmos]"
    exit 1
fi

#
# Start DfsBroker.hadoop
#
PIDFILE=$HYPERTABLE_HOME/run/DfsBroker.$1.pid
LOGFILE=$HYPERTABLE_HOME/log/DfsBroker.$1.log

$HYPERTABLE_HOME/bin/serverup dfsbroker
if [ $? != 0 ] ; then

  if [ "$1" == "hadoop" ] ; then
      nohup $HYPERTABLE_HOME/bin/jrun --pidfile $PIDFILE org.hypertable.DfsBroker.hadoop.main --verbose 1>& $LOGFILE &
  elif [ "$1" == "kosmos" ] ; then
      $VALGRIND $HYPERTABLE_HOME/bin/kosmosBroker --pidfile=$PIDFILE --verbose 1>& $LOGFILE &   
  elif [ "$1" == "local" ] ; then
      $VALGRIND $HYPERTABLE_HOME/bin/localBroker --pidfile=$PIDFILE --verbose 1>& $LOGFILE &    
  else
      echo $"$0: Usage: stat-servers.sh [--initialize] [local|hadoop|kosmos]"      
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
fi
echo "Successfully started DFSBroker ($1)"


#
# Reset state
#
cp $HYPERTABLE_HOME/test/metadata.orig $HYPERTABLE_HOME/test/metadata
rm -rf $HYPERTABLE_HOME/log/hypertable/*
$HYPERTABLE_HOME/bin/dfsclient --eval "rmdir /hypertable"


#
# Start Hyperspace
#
PIDFILE=$HYPERTABLE_HOME/run/Hyperspace.pid
LOGFILE=$HYPERTABLE_HOME/log/Hyperspace.log

$HYPERTABLE_HOME/bin/serverup hyperspace
if [ $? != 0 ] ; then
    nohup $HYPERTABLE_HOME/bin/Hyperspace.Master --pidfile=$PIDFILE --verbose 1>& $LOGFILE &
    sleep 1
    $HYPERTABLE_HOME/bin/serverup hyperspace
    if [ $? != 0 ] ; then
	echo -n "Hyperspace hasn't come up yet, trying again in 5 seconds ..."
	sleep 5
	echo ""
	$HYPERTABLE_HOME/bin/serverup hyperspace
	if [ $? != 0 ] ; then
	    tail -100 $LOGFILE
	    echo "Problem statring Hyperspace";
	    exit 1
	fi
    fi
fi
echo "Successfully started Hyperspace"

#
# If it looks like the master was not initialized, then go ahead and do it.
#
$HYPERTABLE_HOME/bin/hyperspace --eval "exists /hypertable/meta" >& /dev/null
if [ $? != 0 ] ; then
    $HYPERTABLE_HOME/bin/Hypertable.Master --initialize
    if [ $? != 0 ] ; then
        echo "Problem initializing Hypertable.Master"
	exit 1
    fi
    echo "Successfully initialized Hypertable.Master"
fi


#
# Start Hypertable.Master
#
PIDFILE=$HYPERTABLE_HOME/run/Hypertable.Master.pid
LOGFILE=$HYPERTABLE_HOME/log/Hypertable.Master.log

$HYPERTABLE_HOME/bin/serverup master
if [ $? != 0 ] ; then
    nohup $HYPERTABLE_HOME/bin/Hypertable.Master --pidfile=$PIDFILE --verbose 1>& $LOGFILE &
    sleep 1
    $HYPERTABLE_HOME/bin/serverup master
    if [ $? != 0 ] ; then
	echo -n "Hypertable.Master hasn't come up yet, trying again in 5 seconds ..."
	sleep 5
	echo ""
	$HYPERTABLE_HOME/bin/serverup master
	if [ $? != 0 ] ; then
	    tail -100 $LOGFILE
	    echo "Problem statring Hypertable.Master";
	    exit 1
	fi
    fi
fi
echo "Successfully started Hypertable.Master"


#
# If the tables have not been created, then create them
#
for table in Test1 Test2 Test3 ; do
    $HYPERTABLE_HOME/bin/hyperspace --eval "exists /hypertable/tables/$table" >& /dev/null
    if [ $? != 0 ] ; then
	CMDFILE=/tmp/hypertable.tests.$$
	echo "create table $table $HYPERTABLE_HOME/test/$table.xml" > $CMDFILE
	echo "quit" >> $CMDFILE
	$HYPERTABLE_HOME/bin/hypertable < $CMDFILE >& /dev/null
	if [ $? != 0 ] ; then
	    for pidfile in $HYPERTABLE_HOME/run/*.pid ; do
		kill -9 `cat $pidfile`
		rm $pidfile
	    done
	else
	    echo "Successfully created table $table."
	fi
	rm $CMDFILE
    fi
done


#
# Start Hypertable.RangeServer
#
PIDFILE=$HYPERTABLE_HOME/run/Hypertable.RangeServer.pid
LOGFILE=$HYPERTABLE_HOME/log/Hypertable.RangeServer.log

$HYPERTABLE_HOME/bin/serverup rangeserver
if [ $? != 0 ] ; then
    nohup $HYPERTABLE_HOME/bin/Hypertable.RangeServer --pidfile=$PIDFILE --metadata=$HYPERTABLE_HOME/test/metadata --verbose 1>& $LOGFILE &
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