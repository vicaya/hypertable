export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)

port=38090
pidfile="$HYPERTABLE_HOME/run/MonitoringServer.pid"
log="$HYPERTABLE_HOME/log/MonitoringServer.log"
monitoring_script_dir="$HYPERTABLE_HOME/Monitoring"
config_file="${monitoring_script_dir}/app/config/config.yml"
rack_file="${monitoring_script_dir}/config.ru"

start_monitoring() {
  cd ${monitoring_script_dir}
  command="thin -p ${port} -e production -P ${pidfile} -l ${log}  -R ${rack_file} -d start"
  $command 
}

stop_monitoring() {
  cd ${monitoring_script_dir}
  command="thin -p ${port} -e production -P ${pidfile} -l ${log} -R ${rack_file} -d stop"
  $command
}

