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

#
# Runs a shell command on all slave hosts.
#
# Environment Variables
#
#   HYPERTABLE_SLAVES    File naming remote hosts.
#     Default is ${HYPERTABLE_CONF_DIR}/slaves.
#   HYPERTABLE_CONF_DIR  Alternate conf dir. Default is ${HYPERTABLE_HOME}/conf.
#   HYPERTABLE_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   HYPERTABLE_SSH_OPTS Options passed to ssh when running remote commands.
##

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


usage="Usage: slaves.sh [--config confdir] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in 
# hadoop-env.sh. Save it here.
HOSTLIST=$HYPERTABLE_SLAVES

if [ "$HOSTLIST" = "" ]; then
  if [ "$HYPERTABLE_SLAVES" = "" ]; then
    export HOSTLIST="${HYPERTABLE_HOME}/conf/slaves"
  else
    export HOSTLIST="${HYPERTABLE_SLAVES}"
  fi
fi

for slave in `cat "$HOSTLIST"`; do
 ssh $HYPERTABLE_SSH_OPTS $slave $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
 if [ "$HYPERTABLE_SLAVE_SLEEP" != "" ]; then
   sleep $HYPERTABLE_SLAVE_SLEEP
 fi
done

wait
