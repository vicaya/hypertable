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

port=38090
pidfile="$HYPERTABLE_HOME/run/MonitoringServer.pid"
log="$HYPERTABLE_HOME/log/MonitoringServer.log"
monitoring_script_dir="$HYPERTABLE_HOME/Monitoring"
config_file="${monitoring_script_dir}/app/config/config.yml"

start_monitoring() {
  cd ${monitoring_script_dir}
  echo ":production:
        :hypertable_home: \"${HYPERTABLE_HOME}\"" > ${config_file}
  
  command="thin -c ${HYPERTABLE_HOME}-p ${port} -e production -P ${pidfile} -l ${log} -d -R config.ru start"
  $command 
}

stop_monitoring() {
  cd ${monitoring_script_dir}
  command="thin -p ${port} -e production -P ${pidfile} -l ${log}  -d -R config.ru stop"
  $command
}
