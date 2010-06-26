#!/usr/bin/env bash
#
# Copyright 2010 Sanjit Jhala (Hypertable, Inc.)
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

usage() {
  echo ""
  echo "usage: stop-monitoring.sh"
  echo ""
}
pidfile="$HYPERTABLE_HOME/Monitoring/tmp/pids/server.pid"
pid=`cat $pidfile 2> /dev/null`
echo "Killing server running with pid $pid"
kill -9 $pid
command="$HYPERTABLE_HOME/Monitoring/script/server"
ret=`ps -ef| grep -c grep| grep \'$command\'`
[ $? != 0 ] && exit 0
echo "$command seems to be still running:"
exit 1
