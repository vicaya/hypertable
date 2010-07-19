# this calss is responsible for reading from rs_stats.txt
# Its used for displaying summary on homepage
# @@stats_config should go into yml file

class RangeServerStats

  attr_accessor :time_intervals, :stats_list, :timestamps, :stat_types, :stats_total, :stats_config

  def initialize(opts={ })
    @datadir = opts[:data] || HTMonitoring.config[:data]
    @stats_file =  opts[:file] || "rs_stats.txt"
    @stats_list = []
    @stats_total = { }
    @time_intervals = [1,5,10]
    @stats_config = HTMonitoring.rangeserverstats
    @stat_types = get_stat_types
  end


  def get_stat_types
    @stats_config.keys.sort { |a,b| a.to_s <=> b.to_s}.map { |d| d.to_s}
  end

  def get_timestamps
    if !@stats_list.nil? and !@stats_list.first.nil?
      @timestamps = @stats_list.first.get_timestamps
    end
  end

  def get_stats_list
    begin
      if @stats_list.empty?
        stats_parser = StatsParser.new({:datadir => @datadir, :stats_file => @stats_file, :stat_types => @stat_types, :parser => "RangeServer"})
        @stats_list = stats_parser.parse_stats_file
      end
     rescue Exception => err
      raise err
     end
  end

  def get_stats_totals
    begin
      @stats_list = self.get_stats_list
      self.get_timestamps
      @stats_list.each do |object|
        object.stats.each_with_index do |stat,index|
          stat.each do |key,value|
            next if key == :Timestamps or value.nil? # don't calculate sum for timestamps
            @stats_total[key] ||= { }
            current_total = @stats_total[key][index] || 0
            @stats_total[key][index] = current_total + value
          end
        end
      end
    rescue Exception => err
      raise err
    end
  end

  def get_units(stat_name)
    stat_name = stat_name.to_s
    if @stats_config[stat_name]
      return @stats_config[stat_name][:units]
    end
    return ""
  end

  def get_pretty_title(key)
    title = @stats_config[key.to_sym][:pname]
    title.titleize unless title.nil?
  end

end
