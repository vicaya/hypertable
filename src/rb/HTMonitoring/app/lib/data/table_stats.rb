# Table Stats calls stats parser to read data from table_stats.rb

class TableStats

  @@stats_config =  {
    :cells_read =>  {:type => :B, :stats => [:cells_read, :cells_written],  :units => "",:color => ["#edd400","#73d216"],:pname => "Cells Read"},
    :cells_written => {:type => :B, :stats => [:cells_read, :cells_written],  :units => "", :color => ["#edd400","#73d216"],:pname => "Cells Written"},

    :bloom_filter_accesses => {:type => :B, :stats => [:bloom_filter_accesses, :bloom_filter_maybes],  :units => "",:color => ["#ad7fa8","#73d216"],:pname => "Bloom Filter Accesses"},
    :bloom_filter_maybes => {:type => :B, :stats => [:bloom_filter_accesses, :bloom_filter_maybes],  :units => "", :color => ["#ad7fa8","#73d216"], :pname => "Bloom Filter Maybes"},

    :bloom_filter_memory => {:type => :B, :stats => [:bloom_filter_memory, :block_index_memory], :units => "Bytes",:color => ["#73d216","#5c3566"],:pname => "Bloom Filter Memory"},
    :block_index_memory => {:type => :B, :stats => [:bloom_filter_memory, :block_index_memory], :units => "Bytes",:color => ["#73d216","#5c3566"], :pname => "Block Index Memory"},

    :scans => {:type => :C, :stats => [:scans],  :units => "",:color => ["#cc0000"], :pname => "Scans"},
    :disk_used => {:type => :C, :stats => [:disk_used],  :units => "Bytes",:color => ["#fcaf3e"], :pname => "Disk Used"},
    :memory_used => {:type => :C, :stats => [:memory_used], :units => "Bytes",:color => ["#c17d11"], :pname => "Memory Used"},
    :memory_allocated => {:type => :C, :stats => [:memory_allocated],:units => "Bytes", :immutable => true,:color => ["#888a85"],:pname => "Memory Allocated"},
  }

  @@sort_types = ["name","data"]
  attr_accessor :time_intervals, :stats_list, :timestamps, :stat_types, :stats_total,:config,:stats


  def initialize(opts={ })
    @datadir = opts[:data] || HTMonitoring.config[:data]
    @stats_file =  opts[:file] || "table_stats.txt"
    @stats_list = []
    @stats_total = { }
    @time_intervals = { 1 => "last 1 minute",5 => "last 5 minutes",10 => "last 10 minutes"}
    @stat_types = get_stat_types
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
      stats_parser = StatsParser.new({:datadir => @datadir, :stats_file => @stats_file, :stat_types => @stat_types, :parser => "Table"})
      @stats_list = stats_parser.parse_stats_file
    end
  end

  def get_stats_totals
    if @stats_list.empty?
      @stats_list = self.get_stats_list
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
      @stats_list = self.get_stats_list
    end
    @stat_types ||= get_stat_types

    sort_types = ["data" , "name"] # not using now
    selected_sort = opts[:sort_by] || sort_types[0]
    selected_stat = opts[:stat] || @stat_types[0]
    timestamp = opts[:timestamp_index] || 10

    graph_data[:time_intervals] = @time_intervals
    graph_data[:keys] = { } # add keys here JS will draw it in drop down , figure better way of doing this
    @stat_types.each do |stat_type|
      graph_data[:keys][:"#{stat_type}"] ||= { }
      graph_data[:keys][:"#{stat_type}"] = @@stats_config[:"#{stat_type}"][:pname]
    end

    chart_type = get_chart_type(selected_stat)

    graph_data[:"graph"] = { }
    graph_data[:"graph"][:"title"] = timestamp.to_s+" minutes average" # title of the graph
    graph_data[:"graph"][:"vaxis"]={ }
    graph_data[:"graph"][:"vaxis"][:"title"] = "Tables"
    graph_data[:"graph"][:"haxis"] = { }
    graph_data[:"graph"][:"haxis"][:"title"] = chart_type[:units]
    graph_data[:"graph"][:"sort_types"] = @@sort_types
    graph_data[:"graph"][:"colors"] = chart_type[:color] # colors for the graph
    graph_data[:"graph"][:"stats"] = { }
    chart_type[:stats].each do |stat_type|
      graph_data[:"graph"][:"stats"][:"#{stat_type}"] ||= { }
      graph_data[:"graph"][:"stats"][:"#{stat_type}"] = @@stats_config[:"#{stat_type}"][:pname]
    end

    graph_data[:"graph"][:"data"] = { } #holds the data necessary to draw the graph
    graph_data[:"graph"][:"size"] = @stats_list.size

    timestamp_index = @time_intervals.keys.index(timestamp)

    @stats_list.each do |stat_list|
      chart_type[:stats].each do |stat_type|
        graph_data[:"graph"][:"data"][:"#{stat_list.id}"] ||= []
        graph_data[:"graph"][:"data"][:"#{stat_list.id}"].push(stat_list.stats[timestamp_index][:"#{stat_type}"])
      end
    end

    graph_data.to_json
  end


  def get_units(stat_name)
    stat_name = stat_name.to_sym
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
