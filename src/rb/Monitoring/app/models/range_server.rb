# RRDtool backed model
class RangeServer 
  #class methods from module
  extend FileReader
  
  PATH_TO_FILE = "../../../run/monitoring/"
  ORIGINAL_FILE_NAME = "rs_stats.txt"
  COPY_FILE_NAME = "copy_of_#{ORIGINAL_FILE_NAME}"
  UNIT = FileReader::UNIT
  CHART_A_OPTIONS = FileReader::CHART_A_OPTIONS
  CHART_B_OPTIONS = FileReader::CHART_B_OPTIONS
  CHART_C_OPTIONS = FileReader::CHART_C_OPTIONS  
  
  disk_read_KBps = {:type => :B, :stats => [:disk_read_KBps, :disk_write_KBps], :chart_options => CHART_B_OPTIONS, :units => UNIT[:kbps]}
  disk_read_rate = {:type => :B, :stats => [:disk_read_rate, :disk_write_rate], :chart_options => CHART_B_OPTIONS, :units => UNIT[:rwps]}
  net_recv_KBps = {:type => :B, :stats => [:net_recv_KBps, :net_send_KBps], :chart_options => CHART_B_OPTIONS, :units => UNIT[:kbps]}
  disk_read_KBps = {:type => :B, :stats => [:disk_read_KBps, :disk_write_KBps], :chart_options => CHART_B_OPTIONS, :units => UNIT[:kbps]}

  bytes_read = {:type => :B, :stats => [:bytes_read, :bytes_written], :chart_options => CHART_B_OPTIONS, :units => UNIT[:bytes]}
  query_cache_accesses = {:type => :B, :stats => [:query_cache_accesses, :query_cache_hits], :chart_options => CHART_B_OPTIONS, :units => UNIT[:abs]}
  loadavg_0 = {:type => :B, :stats => [:loadavg_0, :loadavg_1, :loadavg_2], :chart_options => CHART_B_OPTIONS, :units => UNIT[:loadave]}
  cells_read = {:type => :B, :stats => [:cells_read, :cells_written], :chart_options => CHART_B_OPTIONS, :units => UNIT[:abs]}
  scans = {:type => :B, :stats => [:scans, :syncs], :chart_options => CHART_B_OPTIONS, :units => UNIT[:abs]}
  block_cache_accesses = {:type => :B, :stats => [:block_cache_accesses, :block_cache_hits], :chart_options => CHART_B_OPTIONS, :units => UNIT[:abs]}

  #data structure to determine graph types, and meta data about each element
  STATS_KEY = {
    #type A
    :percent_disk_used => {:type => :A, :stats => [:disk_used, :disk_available], :chart_options => CHART_A_OPTIONS, :units => UNIT[:percent]},
    :percent_mem_used => {:type => :A, :stats => [:mem_used, :mem_total], :chart_options => CHART_A_OPTIONS, :units => UNIT[:percent]},
    :percent_query_cache_memory_used => {:type => :A, :stats => [:query_cache_available_memory, :query_cache_max_memory], :chart_options => CHART_A_OPTIONS, :units => UNIT[:percent]},    
    :percent_block_cache_memory_used => {:type => :A, :stats => [:block_cache_available_memory, :block_cache_max_memory], :chart_options => CHART_A_OPTIONS, :units => UNIT[:percent]},
    
    #type B
    :disk_read_KBps => disk_read_KBps,
    :disk_write_KBps => disk_read_KBps,
    
    :disk_read_rate => disk_read_rate,
    :disk_write_rate => disk_read_rate,  
  
    :net_recv_KBps => net_recv_KBps,
    :net_send_KBps => net_recv_KBps,  
  
    :bytes_read => bytes_read,
    :bytes_written => bytes_read,
  
    :query_cache_accesses => query_cache_accesses,
    :query_cache_hits => query_cache_accesses,

    :loadavg_0 => loadavg_0,
    :loadavg_1 => loadavg_0, 
    :loadavg_2 => loadavg_0, 
    
    :cells_read => cells_read,
    :cells_written => cells_read, 

    :scans => scans,
    :syncs => scans, 

    :block_cache_accesses => block_cache_accesses,
    :block_cache_hits => block_cache_accesses, 

    # TYPE C
    :virtual_machine_size => {:type => :C, :stats => [:vm_size], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb]},
    :virtual_machine_resident => {:type => :C, :stats => [:vm_resident], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb]},
    :updates => {:type => :C, :stats => [:updates], :chart_options => CHART_C_OPTIONS, :units => UNIT[:ab]},
    :cpu_percent => {:type => :C, :stats => [:cpu_pct], :chart_options => CHART_C_OPTIONS, :units => UNIT[:percent]},
    :block_cache_max_memory => {:type => :C, :stats => [:block_cache_max_memory], :chart_options => CHART_C_OPTIONS, :units => UNIT[:bytes]},
    
    # also have type A percent from these
    :disk_used => {:type => :C, :stats => [:disk_used], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb]},
    :mem_used => {:type => :C, :stats => [:mem_used], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb]},
    :query_cache_available_memory => {:type => :C, :stats => [:query_cache_available_memory], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb]},
    :query_cache_max_memory => {:type => :C, :stats => [:query_cache_max_memory], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb]},
    :block_cache_available_memory => {:type => :C, :stats => [:block_cache_available_memory], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb]},
    :block_cache_max_memory => {:type => :C, :stats => [:block_cache_max_memory], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb]},

    #todo: immutable 
    :disk_available => {:type => :C, :stats => [:disk_available], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb], :immutable => true},
    :mem_total => {:type => :C, :stats => [:mem_total], :chart_options => CHART_C_OPTIONS, :units => UNIT[:kb], :immutable => true},
    :clock_mhz => {:type => :C, :stats => [:clock_mhz], :chart_options => CHART_C_OPTIONS, :units => UNIT[:mhz], :immutable => true},
    :num_cores => {:type => :C, :stats => [:num_cores], :chart_options => CHART_C_OPTIONS, :units => UNIT[:ab], :immutable => true},
    :num_ranges => {:type => :C, :stats => [:num_ranges], :chart_options => CHART_C_OPTIONS, :units => UNIT[:ab], :immutable => true}
    
  }

  STAT_TO_RRD_KEY = {
    :num_ranges => "num_ranges",
    :syncs => "syncs",
    :scans => "scans",
    
    :cells_read => "cells_read",
    :bytes_read => "bytes_read",
    :cells_written => "cells_written",
    :bytes_written => "bytes_written",

    :query_cache_accesses => "q_c_accesses",
    :query_cache_hits => "q_c_hits",
    :block_cache_accesses => "b_c_accesses",
    :block_cache_hits => "b_c_hits",

    :query_cache_max_memory => "q_c_max_mem",
    :query_cache_available_memory => "q_c_avail_mem",

    :block_cache_available_memory => "b_c_avail_mem",
    :block_cache_max_memory => "b_c_max_mem"
  }

  def self.get_stat_types
    STATS_KEY.keys.sort {|a,b| a.to_s <=> b.to_s}.map {|d| d.to_s}
  end
  
  def self.get_chart_type stat
    stat = stat.to_sym
    STATS_KEY[stat]
  end
  
  def initialize (id=nil)
    @id = id
    @data = {}
  end
  
  attr_accessor :id, 
                :timestamps, 
                :data
  
  alias name id

  # RangeServer.get_stats.first.get_value("num_ranges", 0, true)
  def get_value(data_name, time_index, show_units)
    stat_key = STATS_KEY[:"#{data_name}"]
    value = nil

    case stat_key[:type]
    when :A
      #handle nil or of div by zero
      value = self.data[stat_key[:stats][0]][time_index] / (self.data[stat_key[:stats][1]][time_index] * 1.0)
      value = round_to(value, 4) * 100
    when :B
      value = self.data[stat_key[:stats][0]][time_index]
    when :C
      value = self.data[stat_key[:stats][0]][time_index]
    else
      value = ""
    end
    
    if show_units
      unit = stat_key[:units]
      if unit == "%"
        value.to_s + "#{stat_key[:units]}"
      else
        value.to_s + " #{stat_key[:units]}"
      end
    else
      value
    end    
  end
  
  def self.get_units(stat_name)
    STATS_KEY[:"#{stat_name}"][:units]
  end
  
  #utiliity  
  def self.round_to(val, x)
    (val * 10**x).round.to_f / 10**x
  end

end
