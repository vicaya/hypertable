# Table Stats calls stats parser to read data from table_stats.rb

class TableStats

  attr_accessor :time_intervals, :stats_list, :timestamps, :stat_types, :stats_total,:stats_config


  def initialize(opts={ })
    @datadir = opts[:data] || HTMonitoring.config[:data]
    @stats_config = HTMonitoring.tablestats
    @stats_file =  opts[:file] || "table_stats.txt"
    @stats_list = []
    @stats_total = { }
    @timestamps = []
    @time_intervals = { 1 => "last 1 minute",5 => "last 5 minutes",10 => "last 10 minutes"}
    @stat_types = get_stat_types
    @sort_types = ["Name","Value"]
    @graph_data = { } # used for js to draw graphs
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
    if @stats_list.empty?
      stats_parser = StatsParser.new({:datadir => @datadir, :stats_file => @stats_file, :stat_types => @stat_types, :parser => "Table"})
      @stats_list = stats_parser.parse_stats_file
    end
  end

  def get_time_intervals
    time_intervals.keys.sort()
  end

  def get_time_labels
    time_intervals.sort().map { |interval| interval[1]}
  end

  def get_stats_totals
    begin
      get_stats_list
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
    rescue Exception => err
      raise err
    end
  end


  def get_graph_data(opts={ })
    begin
      if @stats_list.empty?
        get_stats_list
        get_timestamps
      end
      @stat_types ||= get_stat_types

      selected_sort = opts[:sort_by] || @sort_types[0]
      selected_stat = opts[:stat] || @stat_types[0]
      timestamp = opts[:timestamp_index] || 10

      get_graph_stat_keys
      @graph_data[:time_intervals] = @time_intervals

      chart_type = get_chart_type(selected_stat)
      get_graph_meta_data(selected_stat,timestamp,chart_type)
      get_graph_stat_data(timestamp,chart_type,selected_sort)
      @graph_data.to_json
    rescue Exception => err
      @graph_data["graph"] ||= { }
      @graph_data["graph"]["error"] = err.message
    end
  end

  def get_graph_stat_data(timestamp,chart_type,selected_sort)
    timestamp_index = get_time_intervals.index(timestamp)
    if @timestamps.size > timestamp_index
      @graph_data[:graph][:data] = { } #holds the data necessary to draw the graph
      stat_type = chart_type[:pair].first # removed the pairing of stats, its now only has one value
      data = { }
      @stats_list.each do |stat_list|
        data["#{stat_list.id}"] = stat_list.stats[timestamp_index][:"#{stat_type}"]
      end
      if(selected_sort == "Value" )
        @graph_data[:graph][:data] = data.sort{ |a,b| a[1] <=> b[1]}
      else
        @graph_data[:graph][:data] = data.sort #sorting by name default
      end
    else
      @graph_data[:graph][:error] = "There is no data for the  #{@time_intervals[timestamp]}"
    end
  end

  # graph title colors for bars , pretty names for stats selected
  def get_graph_meta_data(selected_stat,timestamp,chart_type)

    @graph_data[:"graph"] = { }
    @graph_data[:"graph"][:"title"] = timestamp.to_s+" minutes average" # title of the graph
    @graph_data[:"graph"][:"vaxis"]={ }
    @graph_data[:"graph"][:"vaxis"][:"title"] = "Tables"
    @graph_data[:"graph"][:"haxis"] = { }
    @graph_data[:"graph"][:"haxis"][:"title"] = chart_type[:units]
    @graph_data[:"graph"][:"sort_types"] = @sort_types
    puts chart_type[:color]
    @graph_data[:"graph"][:"colors"] = chart_type[:color] # colors for the graph
    @graph_data[:"graph"][:"stats"] = { }
    chart_type[:pair].each do |stat_type|
      @graph_data[:"graph"][:"stats"][:"#{stat_type}"] ||= { }
      @graph_data[:"graph"][:"stats"][:"#{stat_type}"] = @stats_config[:"#{stat_type}"][:pname]
    end


    @graph_data[:"graph"][:"size"] = @stats_list.size
  end

  # add keys here JS will draw it in drop down
  def get_graph_stat_keys
    @graph_data[:stats] = { }
    @stat_types.each do |stat_type|
      @graph_data[:stats][:"#{stat_type}"] ||= { }
      @graph_data[:stats][:"#{stat_type}"] = @stats_config[:"#{stat_type}"][:pname]
    end
  end

  def get_units(stat_name)
    stat_name = stat_name.to_sym
    if @stats_config[stat_name]
      return @stats_config[stat_name][:units]
    end
    return ""
  end

  def get_chart_type(stat)
    stat = stat.to_sym
    @stats_config[stat]
  end

  def get_pretty_title(key)
    title = @stats_config[key.to_sym][:pname]
    title.titleize unless title.nil?
  end


end
