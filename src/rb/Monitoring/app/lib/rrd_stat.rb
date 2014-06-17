# Author : Sriharsha Chintalapani(harsha@defun.org)

require "#{File.dirname(__FILE__)}/graph.rb"
require "#{File.dirname(__FILE__)}/stats_json.rb"
class RRDStat

  attr_accessor :stats_total, :stats_type,:time_intervals,:selected_stat, :chart_type

  def initialize(opts={ })
    @rrddir = opts[:rrddir] || HTMonitoring.config[:data]
    @table_rrd_dir = @rrddir+"tables"
    @rs_rrd_dir = @rrddir+"rangeservers"
    @server_ip_file = "rs_map.txt"
    @stats_config = HTMonitoring.rrdstats
    @table_stats_config = HTMonitoring.tablestats
    @stat_types = get_stat_types
    @table_stat_types = get_table_stat_types
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
    @server_file_list = { }
    @server_ip_list = { }
    @graph_data = { }
  end


  def get_table_info
    json = StatsJson.new(:file => 'table_summary.json',:data => '/opt/hypertable/0.9.4.3/run/monitoring/')
    range_servers = json.parse_stats_file
    range_servers['TableSummary']['tables'].each do |server|
      @server_ip_list[:"#{server['id']}"] = server['name']
    end
    @server_ip_list                  
  end

  def get_rs_info
    json = StatsJson.new(:file => 'rangeserver_summary.json',:data => '/opt/hypertable/0.9.4.3/run/monitoring/')
    range_servers = json.parse_stats_file
    range_servers['RangeServerSummary']['servers'].each do |server|
      @server_ip_list[:"#{server['location']}"] = server['ip']
    end
    @server_ip_list                  
  end

  def get_stat_types
    @stats_config.keys.sort { |a,b| a.to_s <=> b.to_s}.map { |d| d.to_s}
  end

  def get_table_stat_types
    @table_stats_config.keys.sort { |a,b| a.to_s <=> b.to_s}.map { |d| d.to_s}
  end

  def get_rs_rrd_file(rs)
    @rs_rrd_dir+"/"+rs.to_s+"_stats_v0.rrd"
  end

  def get_table_rrd_file(table)
    @table_rrd_dir+"/"+table.to_s.sub("_","/")+"_table_stats_v0.rrd"
  end

  def get_server_list
    begin
      @graph_data[:servers] = get_rs_info
      @graph_data[:stats] = get_stat_types
    rescue Exception => err
      @graph_data[:graph] = { }
      @graph_data[:graph][:error] = err.message
    end
    @graph_data.to_json
  end

  def get_table_list
    begin
      @graph_data[:servers] = get_table_info
      @graph_data[:stats] = get_table_stat_types
    rescue Exception => err
      @graph_data[:graph] = { }
      @graph_data[:graph][:error] = err.message
    end
    @graph_data.to_json
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
      get_rs_info
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
        rrd_file = get_rs_rrd_file(rs)
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

  def get_table_stat_image(server,stat,start_time,end_time)
    begin
      rrd_graph_data = []
      get_table_info
      range_servers = { }

      if (server.downcase == "all")
        range_servers = @server_ip_list
      else
        range_servers[:"#{server}"] = @server_ip_list[:"#{server}"]
      end

      title = @table_stats_config[:"#{stat}"][:pname]
      stats_pair = @table_stats_config[:"#{stat}"][:pair]
      index = 0
      range_servers.each_pair do |rs, ip|
        rrd_file = get_table_rrd_file(rs)
        puts "harsha "+rrd_file
        stats_pair.each do |pstat|
          name = @table_stats_config[:"#{pstat}"][:pname]
          color = @table_stats_config[:"#{pstat}"][:color].first
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
        smooth = temp.trendnan period/48.0
        g.line   line, smooth, label, color
        g.gprint temp
      end
    end
    rrd_graph.image_data
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
