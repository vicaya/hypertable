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
# Stop HdfsBroker
#
if [ -f  $HYPERTABLE_HOME/run/HdfsBroker.pid ] ; then
    kill -9 `cat  $HYPERTABLE_HOME/run/HdfsBroker.pid`
    rm $HYPERTABLE_HOME/run/HdfsBroker.pid
fi

#
# Stop Hyperspace
#
if [ -f  $HYPERTABLE_HOME/run/Hyperspace.pid ] ; then
    kill -9 `cat  $HYPERTABLE_HOME/run/Hyperspace.pid`
    rm $HYPERTABLE_HOME/run/Hyperspace.pid
fi

#
# Stop Hypertable.Master
#
if [ -f  $HYPERTABLE_HOME/run/Hypertable.Master.pid ] ; then
    kill -9 `cat  $HYPERTABLE_HOME/run/Hypertable.Master.pid`
    rm $HYPERTABLE_HOME/run/Hypertable.Master.pid
fi

