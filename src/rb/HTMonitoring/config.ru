require 'pathname'

@root = Pathname.new(File.dirname(__FILE__)).expand_path

require @root.join('htmonitoring')

#FileUtils.mkdir_p @root.join('log').to_s unless File.exists?(@root.join('log'))
#log = File.new(@root.join("log/sinatra.log").to_s, "a")
#$stdout.reopen(log)
#$stderr.reopen(log)
run HTMonitoring::Admin
