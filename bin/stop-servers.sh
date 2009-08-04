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

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

stop_server $HYPERTABLE_HOME/run/ThriftBroker.pid
stop_server $HYPERTABLE_HOME/run/DfsBroker.*.pid
stop_server $HYPERTABLE_HOME/run/Hypertable.RangeServer.pid
stop_server $HYPERTABLE_HOME/run/Hypertable.Master.pid
stop_server $HYPERTABLE_HOME/run/Hyperspace.pid
sleep 1
wait_for_server_shutdown thriftbroker "thrift broker" &
wait_for_server_shutdown dfsbroker "DFS broker" &
wait_for_server_shutdown rangeserver "range server" &
wait_for_server_shutdown master "hypertable master" &
wait_for_server_shutdown hyperspace "hyperspace" &
wait
