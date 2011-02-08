class StatsJson
  def initialize(opts={ })
    raise ArgumentError if opts[:file].nil?
    @datadir = opts[:data] || HTMonitoring.config[:data]
    @stats_file = @datadir+opts[:file]
  end

  def parse_stats_file
    begin
      json_file = File.open("#{@stats_file}","r")
      records = JSON.parse(json_file.read)
      return records
    end
  end
end
