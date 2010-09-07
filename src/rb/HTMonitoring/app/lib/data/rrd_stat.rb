# Author : Sriharsha Chintalapani(harsha@defun.org)

require "#{File.dirname(__FILE__)}/../errand.rb"
require "#{File.dirname(__FILE__)}/../graph.rb"
class RRDStat

  attr_accessor :stats_total, :stats_type,:time_intervals,:selected_stat, :chart_type

  def initialize(opts={ })
    @rrddir = opts[:rrddir] || HTMonitoring.config[:data]
    @server_ip_file = "rs_map.txt"
    @stats_config = HTMonitoring.rrdstats
    @stat_types = get_stat_types
    @sort_types = ["Name","Value"]
    @time_intervals = {
      1 => "last 1 minute",
      5 => "last 5 minutes",
      10 => "last 10 minutes",
      30 => "last 30 minutes",
      60 => "last 1 hour",
      120 => "last 2 hours",
      360 => "last 6 hours",
      720 => "last 12 hours",
      1440 => "last day",
      2880 => "last 2 days",
      4320 => "last 3 days",
      10080 => "last 1 week",
      20160 => "last 2 weeks",
      43200 => "last month",
      86400 => "last 2 months",
      129600 => "last 3 months",
      259200 => "last 6 months",
      525600 => "last 12 months",
      613200 => "last 18 months"

    }
    @stats_total = { }
    @server_file_list = { }
    @server_ip_list = { }
    @graph_data = { }
  end


  def get_server_files
    rrdglob = "#{@rrddir}*.rrd"
    Dir.glob(rrdglob).sort.uniq.map do |rrdfile|
      parts         = rrdfile.gsub(/#{@rrddir}/, '').split('_')
      instance_name = parts[0]
      @server_file_list[:"#{instance_name}"] = rrdfile
    end
  end


  def get_server_info
    server_parser = StatsParser.new({:datadir => @rrddir, :stats_file => @server_ip_file })
    @server_ip_list = server_parser.parse_rs_mapping_file
    get_server_files
    @server_file_list.keys.each do |server|
      if !@server_ip_list.has_key?(server)
        @server_ip_list[:"#{server}"] = server
      end
    end
    @server_ip_list
  end


  def get_stat_types
    @stats_config.keys.sort { |a,b| a.to_s <=> b.to_s}.map { |d| d.to_s}
  end


  def get_server_list
    begin
      @graph_data[:servers] = get_server_info
      @graph_data[:stats] = get_stat_types
    rescue Exception => err
      @graph_data[:graph] = { }
      @graph_data[:graph][:error] = err.message
    end
    @graph_data.to_json
  end


  def get_graph_data(opts={ })
    begin
      @stat_types ||= get_stat_types
      @selected_stat = opts[:stat] || @stat_types[0]
      selected_sort = opts[:sort_by] || @sort_types[0]
      timestamp_index = opts[:timestamp_index] || 10

      @chart_type = get_chart_type(@selected_stat)
      if @stats_total.empty? or @stats_total.nil?
        fetch_data({ :timestamp => timestamp_index})
      end
      get_graph_stat_keys
      get_graph_meta_data(timestamp_index)
      get_graph_stat_data(timestamp_index,selected_sort)
      @graph_data.to_json
    rescue Exception => err
      @graph_data["graph"] ||= { }
      @graph_data["graph"]["error"] = err.message
    end
  end


  def get_graph_stat_data(timestamp_index,selected_sort)
    @graph_data[:graph][:data] = { } #holds the data necessary to draw the graph
    error_flag = 1
    data = { }
    stat_type = @chart_type[:pair].first # removed the pairing of stats, its now only has one value
    @stats_total.each do |server,stats|
      if !stats[:"#{stat_type}"].nil? or !stats[:"#{stat_type}"] == 0 # this is to check if the stats_total contains 0's and display appropriate error message
          error_flag = 0
          data["#{server}"] = stats[:"#{stat_type}"] # Need to add IP  #TODO
      end
    end
    if error_flag == 1
      @graph_data[:graph][:error] = "There is no data for " + @stats_config[:"#{@selected_stat}"][:pname]+" during "+ @time_intervals[timestamp_index]
    else
      if(selected_sort == "Value" )
        @graph_data[:graph][:data] = data.sort{ |a,b| a[1] <=> b[1]}
      else
        puts data.class
        @graph_data[:graph][:data] = data.sort #sorting by name default
      end
    end

  end


  def get_graph_meta_data(timestamp_index)
    @graph_data[:time_intervals] = @time_intervals
    @graph_data[:graph] = { }
    @graph_data[:graph][:title] = @time_intervals[timestamp_index].gsub("last","value")+" averaged over " # title of the graph i hate it bad hack
    @graph_data[:graph][:vaxis]={ }
    @graph_data[:graph][:vaxis][:title] = "Range Servers"
    @graph_data[:graph][:colors] = @chart_type[:color] # colors for the graph
    @graph_data[:graph][:haxis] = { }
    @graph_data[:graph][:haxis][:title] = @chart_type[:units]
    @graph_data[:graph][:size] = @stats_total.size
    @graph_data[:graph][:stats] = { }

    @chart_type[:pair].each do |stat_type|
      @graph_data[:graph][:stats][:"#{stat_type}"] ||= { }
      @graph_data[:graph][:stats][:"#{stat_type}"] = @stats_config[:"#{stat_type}"][:pname]
    end
  end


  def get_graph_stat_keys
    @graph_data[:stats] = { } # add keys here JS will draw it in drop down , figure better way of doing this
    @stat_types.each do |stat_type|
      @graph_data[:stats][:"#{stat_type}"] ||= { }
      @graph_data[:stats][:"#{stat_type}"] = @stats_config[:"#{stat_type}"][:pname]
    end
  end


  def get_rrd_stat_image(server,stat,start_time,end_time)
    begin
      rrd_graph_data = []
      get_server_info
      range_servers = { }

      if (server.downcase == "all")
          range_servers = @server_ip_list
      else
        range_servers[:"#{server}"] = @server_ip_list[:"#{server}"]
      end

      title = @stats_config[:"#{stat}"][:pname]
      stats_pair = @stats_config[:"#{stat}"][:pair]
      index = 0
      range_servers.each_pair do |rs, ip|
        rrd_file = @server_file_list[:"#{rs}"]
        stats_pair.each do |pstat|
          name = @stats_config[:"#{pstat}"][:pname]
          color = @stats_config[:"#{pstat}"][:color].first
          rrd_graph_data << ["#{ip} #{name}   ", "#{rrd_file}:#{pstat}:AVERAGE",color]
        end
        index = index + 1
      end

      image_data = rrd_make_graph(rrd_graph_data,title,start_time,end_time)
    rescue Exception => err
      raise err
    end
  end


  def rrd_make_graph(rrd_graph_data,title,start_time,end_time)
    w = 900
    h = 260
    line = 2
    period = end_time.to_f - start_time.to_f
    expr = RRD::Graph::Expr

    rrd_graph = RRD::Graph.new w, h, title, start_time, end_time  do |g|
      rrd_graph_data.each do |label, path, color|
        temp   = expr.read "#{path}"
        smooth = temp.trend period/48.0
        g.line   line, smooth, label, color
        g.gprint temp
      end
    end
    rrd_graph.image_data
  end


  def fetch_data(opts={ })
    rrdglob = "#{@rrddir}*.rrd"
    data = []
    timestamp = opts[:timestamp] || 10
    secs = timestamp * 60
    start = opts[:start] || (Time.now - secs).to_i
    finish = start
    resolution =opts[:resolution] ||  (secs / 10)

    Dir.glob(rrdglob).sort.uniq.map do |rrdfile|
      parts         = rrdfile.gsub(/#{@rrddir}/, '').split('_')
      instance_name = parts[0]
      rrd           = Errand.new(:filename => rrdfile)
      data << { :instance => instance_name,
                :rrd => rrd,
                :start  => opts[:start] || start,
                :finish => opts[:finish] || finish,
                :resolution => opts[:resolution] || resolution
      }
    end
    cal_rs_totals(data)
  end


  def cal_rs_totals(datas)
    datas.each do |data|
      fetch = data[:rrd].fetch(:function => "AVERAGE",
                               :start => data[:start],
                               :finish => data[:finish],
                               :resolution => data[:resolution])
      instance_name = data[:instance]
      if fetch
        @stats_total[:"#{instance_name}"] ||= { }
        rrd_data = fetch[:data]
        @chart_type[:pair].each do |source|
          metric = rrd_data["#{source}"]
          #take the first value when averaging out the second value in metric is ridiculously large number
          unless metric and metric.first.nil?
            @stats_total[:"#{instance_name}"][:"#{source}"] = metric.first
          end
        end
      end
    end
  end


  def get_units(stat_name)
    stat_name = stat_name.to_s
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
    title.titleize
  end

end
