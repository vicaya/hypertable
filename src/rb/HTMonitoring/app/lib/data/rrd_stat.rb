# Author : Sriharsha Chintalapani(harsha@defun.org)
require "#{File.dirname(__FILE__)}/../errand.rb" 
class RRDStat
  @@stats_config = {
    #type A

    :percent_mem_used => {:type => :A, :stats => [:mem_used, :mem_total],  :units => "%",:pname => "Percent Mem Used",:color => ["#cc0000","#73d216"]},
    :percent_query_cache_memory_used => {:type => :A, :stats => [:q_c_avail_mem, :q_c_max_mem], :units => "%",:pname => "Percent Query Cache Memory Used" ,:color => ["#8ae234","#ef2929"]},

    #type B

    :net_recv_KBps => {:type => :B, :stats => [:net_recv_KBps, :net_sent_KBps], :color => ["#c17d11","#888a85"] , :units => "KBps",:pname => "Net Recv KBps"},
    :net_sent_KBps => {:type => :B, :stats => [:net_recv_KBps, :net_sent_KBps], :color => ["#c17d11","#888a85"] , :units => "KBps",:pname => "Net Sent KBps"},

    :bytes_read => {:type => :B, :stats => [:bytes_read, :bytes_written], :color => ["#c17d11","#888a85"], :units => "Bytes",:pname => "Bytes Read"},
    :bytes_written => {:type => :B, :stats => [:bytes_read, :bytes_written], :color => ["#c17d11","#888a85"], :units => "Bytes",:pname => "Bytes Written"},

    :q_c_accesses => {:type => :B, :stats => [:q_c_accesses, :q_c_hits], :color => ["#8ae234","#ef2929"], :units => "",:pname => "Query Cache Accesses"},
    :q_c_hits => {:type => :B, :stats => [:q_c_accesses, :q_c_hits], :color => ["#8ae234","#ef2929"], :units => "",:pname => "Query Cache Hits"},

    :loadavg => {:type => :B, :stats => [:loadavg], :color => ["#3465a4"], :units => "",:pname => "Load Avg"},

    :cells_read => {:type => :B, :stats => [:cells_read, :cells_written], :color =>["#c17d11","#888a85"] , :units => "",:pname => "Cells Read"},
    :cells_written => {:type => :B, :stats => [:cells_read, :cells_written], :color =>["#c17d11","#888a85"] , :units => "",:pname => "Cells Written"},

    :scans => {:type => :C, :stats => [:scans],  :units => "",:color => ["#cc0000"], :pname => "Scans"},
    :syncs => {:type => :C, :stats => [:syncs,:updates],  :units => "",:color => ["#cc0000","#5c3566","#ad7fa8"], :pname => "Syncs"},
#    :updates => {:type => :C, :stats => [:syncs,:updates], :color => ["#cc0000","#5c3566","#ad7fa8"],:units => "",:pname => "Updates" },

    :b_c_accesses => {:type => :B, :stats => [:b_c_accesses, :b_c_hits], :color =>["#cc0000","#5c3566"], :units => "",:pname => "Block Cache Access"},
    :b_c_hits => {:type => :B, :stats => [:b_c_accesses, :b_c_hits], :color =>["#cc0000","#5c3566"], :units => "",:pname => "Block Cache Hits"},

    # TYPE C
    :vm_size => {:type => :C, :stats => [:vm_size], :color => ["#ef2929"], :units => "KB" ,:pname => "Virtual Machine Size"},
    :vm_resident => {:type => :C, :stats => [:vm_resident], :color => ["#73d216"], :units => "KB", :pname => "Virtual Machine Resident"},

    :cpu_pct => {:type => :C, :stats => [:cpu_pct], :color => ["#cc0000"], :units =>"percent",:pname => "Cpu Percent"},
    :b_c_max_mem => {:type => :C, :stats => [:b_c_max_memory], :units => "Bytes", :pname => "Block Cache Max Memory",:color => ["#cc0000"]},

    # also have type A percent from these
    :mem_used => {:type => :C, :stats => [:mem_used], :color => ["#fcaf3e"], :units => "KB" ,:pname => "Mem Used"},
    :q_c_avail_mem => {:type => :C, :stats => [:q_c_avail_mem], :color => ["#fcaf3e"], :units => "KB" ,:pname => "Query Cache Available Memory"},
    :q_c_max_mem => {:type => :C, :stats => [:q_c_max_mem], :color => ["#ef2929"], :units => "KB" ,:pname => "Query Cache Max Memory"},

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
    #:disk_read_rate => {:type => :B, :stats => [:disk_read_rate, :disk_write_rate], :color => ["#c17d11","#888a85"], :units => "per second",:pname => "Disk Read Rate"},
    #:disk_write_rate => {:type => :B, :stats => [:disk_read_rate, :disk_write_rate], :color => ["#c17d11","#888a85"], :units => "per second",:pname => "Disk Write Rate"},

  }

  attr_accessor :stats_total, :stats_type,:time_intervals,:selected_stat, :chart_type

  def initialize(opts={ })
    @rrddir = opts[:rrddir] || HTMonitoring.config[:data]
    @stat_types = get_stat_types

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
  end

  def get_stat_types
    @@stats_config.keys.sort { |a,b| a.to_s <=> b.to_s}.map { |d| d.to_s}
  end

  def get_graph_data(opts={ })
    graph_data = { }

    @stat_types ||= get_stat_types
    sort_types = ["data" , "name"] # not using now
    selected_sort = opts[:sort_by] || sort_types[0]
    @selected_stat = opts[:stat] || @stat_types[0]
    timestamp_index = opts[:timestamp_index] || 10
    @chart_type = get_chart_type(@selected_stat)

    if @stats_total.empty? or @stats_total.nil?
      fetch_data({ :timestamp => timestamp_index})
    end


    graph_data[:time_intervals] = @time_intervals
    graph_data[:keys] = { } # add keys here JS will draw it in drop down , figure better way of doing this
    @stat_types.each do |stat_type|
      graph_data[:keys][:"#{stat_type}"] ||= { }
      graph_data[:keys][:"#{stat_type}"] = @@stats_config[:"#{stat_type}"][:pname]
    end


    graph_data[:"graph"] = { }
    graph_data[:"graph"][:"title"] = @time_intervals[timestamp_index].gsub("last","value")+" averaged over " # title of the graph i hate it bad hack
    graph_data[:"graph"][:"vaxis"]={ }
    graph_data[:"graph"][:"vaxis"][:"title"] = "Range Servers"
    graph_data[:"graph"][:"colors"] = chart_type[:color] # colors for the graph
    graph_data[:"graph"][:"haxis"] = { }
    graph_data[:"graph"][:"haxis"][:"title"] = chart_type[:units]
    graph_data[:"graph"][:"size"] = @stats_total.size
    graph_data[:"graph"][:"stats"] = { }

    @chart_type[:stats].each do |stat_type|
      graph_data[:"graph"][:"stats"][:"#{stat_type}"] ||= { }
      graph_data[:"graph"][:"stats"][:"#{stat_type}"] = @@stats_config[:"#{stat_type}"][:pname]
    end

    graph_data[:"graph"][:"data"] = { } #holds the data necessary to draw the graph

    error_flag = 0
    @stats_total.each do |server,stats|
      @chart_type[:stats].each do |stat_type|
        if stats[:"#{stat_type}"].nil? or stats[:"#{stat_type}"] == 0 # this is to check if the stats_total contains 0's and display appropriate error message
          error_flag = 1
       else
          error_flag =0
          graph_data[:"graph"][:"data"][:"#{server}"] ||= []
          graph_data[:"graph"][:"data"][:"#{server}"].push(stats[:"#{stat_type}"])
         end
      end
    end
    if error_flag == 1
      graph_data[:"graph"][:"error"] = "There is no data for " + @@stats_config[:"#{@selected_stat}"][:pname]+" during "+ @time_intervals[timestamp_index]
    end

    graph_data.to_json
  end

  def fetch_data(opts={ })
    rrdglob = "#{@rrddir}*.rrd"
    data = []
    timestamp = opts[:timestamp] || 10
    secs = timestamp * 60
    start = opts[:start] || (Time.now - secs).to_i
    finish = start
    resolution =opts[:resolution] ||  (secs / 10)

    #puts start,finish,resolution

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
      #puts data[:start],data[:finish],data[:resolution]
      fetch = data[:rrd].fetch(:function => "AVERAGE",
                               :start => data[:start],
                               :finish => data[:finish],
                               :resolution => data[:resolution])
      instance_name = data[:instance]
      if fetch
        @stats_total[:"#{instance_name}"] ||= { }
        rrd_data = fetch[:data]
        @chart_type[:stats].each do |source|
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
