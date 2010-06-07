class Table
  # require 'pp'
  # require 'ftools'
  extend FileReader
  
  PATH_TO_FILE = "../../../run/monitoring/"
  ORIGINAL_FILE_NAME = "table_stats.txt"
  COPY_FILE_NAME = "copy_of_#{ORIGINAL_FILE_NAME}"
  UNIT = FileReader::UNIT
  CHART_A_OPTIONS = FileReader::CHART_A_OPTIONS
  CHART_B_OPTIONS = FileReader::CHART_B_OPTIONS
  CHART_C_OPTIONS = FileReader::CHART_C_OPTIONS
  
  
  cells_read = {:type => :B, :stats => [:cells_read, :cells_written], :chart_options => CHART_B_OPTIONS, :units => UNIT[:abs]}
  bloom_filter_accesses = {:type => :B, :stats => [:bloom_filter_accesses, :bloom_filter_maybes], :chart_options => CHART_B_OPTIONS, :units => UNIT[:abs]}
  bloom_filter_memory = {:type => :B, :stats => [:bloom_filter_memory, :block_index_memory, :shadow_cache_memory], :chart_options => CHART_B_OPTIONS, :units => UNIT[:bytes]}
  #data structure to determine graph types, and what graphs to display.
  STATS_KEY = {
    :percent_memory_used => {:type => :A, :stats => [:memory_used, :memory_allocated], :chart_options => CHART_A_OPTIONS, :units => UNIT[:percent]},
    
    :cells_read => cells_read,
    :cells_written => cells_read,
    
    :bloom_filter_accesses => bloom_filter_accesses,
    :bloom_filter_maybes => bloom_filter_accesses,
    
    :bloom_filter_memory => bloom_filter_memory,
    :block_index_memory => bloom_filter_memory,
    :shadow_cache_memory => bloom_filter_memory,

    :scans => {:type => :C, :stats => [:scans], :chart_options => CHART_C_OPTIONS, :units => UNIT[:ab]},
    :bloom_filter_false_positives => {:type => :C, :stats => [:bloom_filter_false_positives], :chart_options => CHART_C_OPTIONS, :units => UNIT[:ab]},
    :disk_used => {:type => :C, :stats => [:disk_used], :chart_options => CHART_C_OPTIONS, :units => UNIT[:bytes]},
    :memory_used => {:type => :C, :stats => [:memory_used], :chart_options => CHART_C_OPTIONS, :units => UNIT[:bytes]}, 

    #todo: immutable
    :memory_alocated => {:type => :C, :stats => [:memory_allocated], :chart_options => CHART_C_OPTIONS, :units => UNIT[:bytes], :immutable => true}
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
                :data #data is a hash containing current stats collected.                
                  # :scans, 
                  # :cells_read, 
                  # :bytes_read, 
                  # :cells_written, 
                  # :bytes_written,
                  # :bloom_filter_accesses,
                  # :bloom_filter_maybes,
                  # :bloom_filter_false_positives,
                  # :bloom_filter_memory,
                  # :block_index_memory,
                  # :shadow_cache_memory,
                  # :memory_used,
                  # :memory_allocated,
                  # :disk_used
  
  #todo: temp until we have a real table name
  alias name id
  
  #todo: RS and Table get_value methods are identical
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

