# this calss is responsible for reading from rs_stats.txt
# Its used for displaying summary
# @@stats_config should go into yml file

class RangeServerStats

  @@stats_config = {
    #type A

    :percent_mem_used => {:type => :A, :stats => [:mem_used, :mem_total],  :units => "%",:pname => "Percent Mem Used",:color => ["#cc0000","#73d216"]},
    :percent_query_cache_memory_used => {:type => :A, :stats => [:query_cache_available_memory, :query_cache_max_memory], :units => "%",:pname => "Percent Query Cache Memory Used" ,:color => ["#8ae234","#ef2929"]},

    #type B

    :disk_read_rate => {:type => :B, :stats => [:disk_read_rate, :disk_write_rate], :color => ["#c17d11","#888a85"], :units => "per second",:pname => "Disk Read Rate"},
    :disk_write_rate => {:type => :B, :stats => [:disk_read_rate, :disk_write_rate], :color => ["#c17d11","#888a85"], :units => "per second",:pname => "Disk Write Rate"},

    :net_recv_KBps => {:type => :B, :stats => [:net_recv_KBps, :net_send_KBps], :color => ["#c17d11","#888a85"] , :units => "KBps",:pname => "Net Recv KBps"},
    :net_send_KBps => {:type => :B, :stats => [:net_recv_KBps, :net_send_KBps], :color => ["#c17d11","#888a85"] , :units => "KBps",:pname => "Net Send KBps"},

    :bytes_read => {:type => :B, :stats => [:bytes_read, :bytes_written], :color => ["#c17d11","#888a85"], :units => "Bytes",:pname => "Bytes Read"},
    :bytes_written => {:type => :B, :stats => [:bytes_read, :bytes_written], :color => ["#c17d11","#888a85"], :units => "Bytes",:pname => "Bytes Written"},

    :query_cache_accesses => {:type => :B, :stats => [:query_cache_accesses, :query_cache_hits], :color => ["#8ae234","#ef2929"], :units => "",:pname => "Query Cache Accesses"},
    :query_cache_hits => {:type => :B, :stats => [:query_cache_accesses, :query_cache_hits], :color => ["#8ae234","#ef2929"], :units => "",:pname => "Query Cache Hits"},

    :loadavg_0 => {:type => :B, :stats => [:loadavg_0, :loadavg_1, :loadavg_2], :color => ["#73d216", "#3465a4","#edd400"], :units => "",:pname => "Load Avg 0"},
    :loadavg_1 => {:type => :B, :stats => [:loadavg_0, :loadavg_1, :loadavg_2], :color => ["#73d216", "#3465a4","#edd400"], :units => "",:pname => "Load Avg 1"},
    :loadavg_2 => {:type => :B, :stats => [:loadavg_0, :loadavg_1, :loadavg_2], :color => ["#73d216", "#3465a4","#edd400"], :units => "",:pname => "Load Avg 2"},

    :cells_read => {:type => :B, :stats => [:cells_read, :cells_written], :color =>["#c17d11","#888a85"] , :units => "",:pname => "Cells Read"},
    :cells_written => {:type => :B, :stats => [:cells_read, :cells_written], :color =>["#c17d11","#888a85"] , :units => "",:pname => "Cells Written"},

    :scans => {:type => :C, :stats => [:scans],  :units => "",:color => ["#cc0000"], :pname => "Scans"},
    :syncs => {:type => :C, :stats => [:syncs,:updates],  :units => "",:color => ["#cc0000","#5c3566","#ad7fa8"], :pname => "Syncs"},
    :updates => {:type => :C, :stats => [:syncs,:updates], :color => ["#cc0000","#5c3566","#ad7fa8"],:units => "",:pname => "Updates" },

    :block_cache_accesses => {:type => :B, :stats => [:block_cache_accesses, :block_cache_hits], :color =>["#cc0000","#5c3566"], :units => "",:pname => "Block Cache Access"},
    :block_cache_hits => {:type => :B, :stats => [:block_cache_accesses, :block_cache_hits], :color =>["#cc0000","#5c3566"], :units => "",:pname => "Block Cache Hits"},

    # TYPE C
    #:virtual_machine_size => {:type => :C, :stats => [:vm_size], :color => ["#ef2929"], :units => "KB" ,:pname => "Virtual Machine Size"},
    #:virtual_machine_resident => {:type => :C, :stats => [:vm_resident], :color => ["#73d216"], :units => "KB", :pname => "Virtual Machine Resident"},

    #:cpu_percent => {:type => :C, :stats => [:cpu_pct], :color => ["#cc0000"], :units =>"percent",:pname => "Cpu Percent"},
    :block_cache_max_memory => {:type => :C, :stats => [:block_cache_max_memory], :units => "Bytes", :pname => "Block Cache Max Memory",:color => ["#cc0000"]},

    # also have type A percent from these
    :mem_used => {:type => :C, :stats => [:mem_used], :color => ["#fcaf3e"], :units => "KB" ,:pname => "Mem Used"},
    :query_cache_available_memory => {:type => :C, :stats => [:query_cache_available_memory], :color => ["#fcaf3e"], :units => "KB" ,:pname => "Query Cache Available Memory"},
    :query_cache_max_memory => {:type => :C, :stats => [:query_cache_max_memory], :color => ["#ef2929"], :units => "KB" ,:pname => "Query Cache Max Memory"},

    #todo: immutable
    :mem_total => {:type => :C, :stats => [:mem_total], :units => "Bytes",:color => ["#5c3566"], :pname => "Mem Total"},
    :clock_mhz => {:type => :C, :stats => [:clock_mhz], :units => "",:color => ["#edd400"], :pname => "Clock Mhz"},
    :num_cores => {:type => :C, :stats => [:num_cores], :units => "",:color => ["#73d216"], :pname => "Num Cores"},
    :num_ranges => {:type => :C, :stats => [:num_ranges], :units => "",:color => ["#3465a4"], :pname => "Num Ranges"},

    # below stats are not accurate at this point. Will be used in the future
    #:disk_used => {:type => :C, :stats => [:disk_used],  :units => UNIT[:kb]},
    #:disk_available => {:type => :C, :stats => [:disk_available],  :units => UNIT[:kb], :immutable => true},
    #:disk_read_KBps => disk_read_KBps,
    #:disk_write_KBps => disk_read_KBps,
    #:percent_disk_used => {:type => :A, :stats => [:disk_used, :disk_available],  :units => UNIT[:percent]},
    #:percent_block_cache_memory_used => {:type => :A, :stats => [:block_cache_available_memory, :block_cache_max_memory],  :units => UNIT[:percent]},
    #:block_cache_available_memory => {:type => :C, :stats => [:block_cache_available_memory],  :units => UNIT[:kb]},
    #:block_cache_max_memory => {:type => :C, :stats => [:block_cache_max_memory],  :units => UNIT[:kb]},

  }


  attr_accessor :time_intervals, :stats_list, :timestamps, :stat_types, :stats_total

  def initialize(opts={ })
    @datadir = opts[:data] || HTMonitoring.config[:data]
    @stats_file =  opts[:file] || "rs_stats.txt"
    @stats_list = []
    @stats_total = { }
    @time_intervals = [1,5,10]
    @stat_types = get_stat_types
    @stats_parser = StatsParser.new({:datadir => @datadir, :stats_file => @stats_file, :stat_types => @stat_types, :parser => "RangeServer"})
  end

  def get_timestamps
    if !@stats_list.nil? and !@stats_list.first.nil?
      @timestamps = @stats_list.first.get_timestamps
    end
  end

  def get_stat_types
    @@stats_config.keys.sort { |a,b| a.to_s <=> b.to_s}.map { |d| d.to_s}
  end

  def get_timestamps
    if !@stats_list.nil? and !@stats_list.first.nil?
      @timestamps = @stats_list.first.get_timestamps
    end
  end

  def get_stats_list
    if @stats_list.empty?
      @stats_list = @stats_parser.parse_stats_file
    end
  end

  def get_stats_totals
    if @stats_list.empty?
      @stats_list = @stats_parser.parse_stats_file
    end
    get_timestamps
    # too many loops?
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
  end


  def get_graph_data(opts={ })
    graph_data = { }
    if @stats_list.empty?
      @stats_list = @stats_parser.parse_stats_file
    end
    @stat_types ||= get_stat_types

    sort_types = ["data" , "name"] # not using now
    selected_sort = opts[:sort_by] || sort_types[0]
    selected_stat = opts[:stat] || @stat_types[0]
    timestamp_index = opts[:timestamp_index] || 2


    graph_data[:keys] = { } # add keys here JS will draw it in drop down , figure better way of doing this
    @stat_types.each do |stat_type|
      graph_data[:keys][:"#{stat_type}"] ||= { }
      graph_data[:keys][:"#{stat_type}"] = @@stats_config[:"#{stat_type}"][:pname]
    end

    chart_type = get_chart_type(selected_stat)
    graph_data[:"graph"] = { }
    graph_data[:"graph"][:"title"] = @time_intervals[timestamp_index].to_s+" minutes average" # title of the graph
    graph_data[:"graph"][:"vaxis"]={ }
    graph_data[:"graph"][:"vaxis"][:"title"] = "Range Servers"
    graph_data[:"graph"][:"colors"] = chart_type[:color] # colors for the graph
    graph_data[:"graph"][:"stats"] = { }

    chart_type[:stats].each do |stat_type|
      graph_data[:"graph"][:"stats"][:"#{stat_type}"] ||= { }
      graph_data[:"graph"][:"stats"][:"#{stat_type}"] = @@stats_config[:"#{stat_type}"][:pname]
    end

    graph_data[:"graph"][:"data"] = { } #holds the data necessary to draw the graph
    graph_data[:"graph"][:"size"] = @stats_list.size
    graph_data[:"graph"][:"haxis"] = { }
    graph_data[:"graph"][:"haxis"][:"title"] = chart_type[:units]


    @stats_list.each do |stat_list|
      chart_type[:stats].each do |stat_type|
        graph_data[:"graph"][:"data"][:"#{stat_list.id}"] ||= []
        graph_data[:"graph"][:"data"][:"#{stat_list.id}"].push(stat_list.stats[timestamp_index][:"#{stat_type}"])
      end
    end
    graph_data.to_json
  end

  def get_units(stat_name)
    stat_name = stat_name.to_s
    if @@stats_config[stat_name]
      return @@stats_config[stat_name][:units]
    end
    return ""
  end

  def get_chart_type(stat)
    stat = stat.to_sym
    @@stats_config[stat]
  end

  def get_pretty_title(key)
    title = @@stats_config[key.to_sym][:pname]
    title.titleize
  end

end
