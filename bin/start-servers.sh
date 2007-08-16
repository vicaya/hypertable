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


#
# Start HdfsBroker
#
PIDFILE=$HYPERTABLE_HOME/run/HdfsBroker.pid
LOGFILE=$HYPERTABLE_HOME/log/HdfsBroker.log

nohup $HYPERTABLE_HOME/bin/jrun --pidfile $PIDFILE org.hypertable.HdfsBroker.main --verbose 1>& $LOGFILE &

sleep 1

$HYPERTABLE_HOME/bin/serverup hdfsbroker
if [ $? != 0 ] ; then
  echo -n "HdfsBroker hasn't come up yet, trying again in 5 seconds ..."
  sleep 5
  echo ""
  $HYPERTABLE_HOME/bin/serverup hdfsbroker
  if [ $? != 0 ] ; then
      echo "Problem statring HdfsBroker";
      exit 1
  fi
fi

#
# Start Hyperspace
#
PIDFILE=$HYPERTABLE_HOME/run/Hyperspace.pid
LOGFILE=$HYPERTABLE_HOME/log/Hyperspace.log

nohup $HYPERTABLE_HOME/bin/jrun --pidfile $PIDFILE org.hypertable.Hyperspace.main --verbose 1>& $LOGFILE &

sleep 1

$HYPERTABLE_HOME/bin/serverup hyperspace
if [ $? != 0 ] ; then
  echo -n "Hyperspace hasn't come up yet, trying again in 5 seconds ..."
  sleep 5
  echo ""
  $HYPERTABLE_HOME/bin/serverup hyperspace
  if [ $? != 0 ] ; then
      echo "Problem statring Hyperspace";
      exit 1
  fi
fi


#
# Start Hypertable.Master
#
PIDFILE=$HYPERTABLE_HOME/run/Hypertable.Master.pid
LOGFILE=$HYPERTABLE_HOME/log/Hypertable.Master.log

nohup $HYPERTABLE_HOME/bin/Hypertable.Master --pidfile=$PIDFILE --verbose 1>& $LOGFILE &

sleep 1

$HYPERTABLE_HOME/bin/serverup master
if [ $? != 0 ] ; then
  echo -n "Hypertable.Master hasn't come up yet, trying again in 5 seconds ..."
  sleep 5
  echo ""
  $HYPERTABLE_HOME/bin/serverup master
  if [ $? != 0 ] ; then
      echo "Problem statring Hypertable.Master";
      exit 1
  fi
fi
