# This class is responsible for reading table_stats and rs_stats file
# removing google charts stuff and adding methods to expose data as json.
# This class handles retrieving stats for both table and range servers


class StatsParser
  attr_accessor :stats_list

  def initialize(opts={ })
    raise ArgumentError if opts[:datadir].nil? and opt[:stats_file].nil?
    @datadir = opts[:datadir]
    @stats_file = @datadir+opts[:stats_file]
    @copy_stats_file = @datadir+"copy_"+opts[:stats_file]
    @stat_types = opts[:stat_types]
    @parser = opts[:parser]
  end

  def parse_stats_file
    begin
      copy_stats_file
      stats_list = []
      #parse copied file here
      file = File.open("#{@copy_stats_file}", "r")
      current_table = '' # to keep track current table parsing
      file.each do |line|
        if line =~ /^(#{@parser}).*=\s*(.*)/
          current_table = Stat.new($2) # assuming $2 as id
          stats_list.push(current_table)
          #list.push current_stat
        elsif line =~ /^\t(.+)=(.+)/
          #please look into util.rb under helpers we have disabled some keys due to invalid stats
          # timestamps should be parsed and they are not part of stat_types
          next if $1 != "Timestamps" and !@stat_types.include?($1)
          key = :"#{$1}"
          values = $2.split(",")
          values.map! do |v|
            if v =~ /\./
              v.to_f  #data can be floats
            else
              v.to_i
            end
          end
          # loop through all the values and put each it in apporiate index in table's stat
          values.each_with_index do |value, index|
            current_table.stats[index] ||= { } # creates a nested hash
            current_table.stats[index][:"#{key}"] = value
          end
        end
      end
      file.close
      return stats_list
    rescue Exception => err
      raise err
    end
  end

  def parse_rs_mapping_file
    begin
      rs_ip_mappings = { }
      copy_stats_file
      file = File.open("#{@copy_stats_file}", "r")
      file.each do |line|
        rs,ip = line.split("=")
        if (!rs.nil? and !ip.nil?)
          rs_ip_mappings[rs.to_sym] = ip.chomp
        end
      end
      rs_ip_mappings
    rescue Exception => err
      raise err
    end
  end

  def copy_stats_file
    # if stats_file exists copy it
    begin
      if File.exists?(@stats_file)
        FileUtils.copy("#{@stats_file}", "#{@copy_stats_file}")
      end
    rescue Exception => err
      raise err
    end
  end

end
