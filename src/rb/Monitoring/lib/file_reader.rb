module FileReader
  TIME_INTERVALS = [1, 5, 10]
  UNIT = {
      :kbps => "KBps", 
      :rwps => "per second", 
      :bytes => "Bytes", 
      :kb => "KB", 
      :loadave => "",# "measure of waiting proc in proc queue",
      :abs => "",  #"absolute numbers",
      :ab => "", #absolute number",
      :percent => "%",
      :mhz => "Mhz"
    }
    
    CHART_A_OPTIONS = {:padding => 95, :legend_height => 8, :bar_width_or_scale => 8, :space_between_bars => 1, :space_between_groups => 2}
    CHART_B_OPTIONS = {:padding => 95, :legend_height => 8, :bar_width_or_scale => 4, :space_between_bars => 1, :space_between_groups => 2}
    CHART_C_OPTIONS = {:padding => 95, :legend_height => 8, :bar_width_or_scale => 8, :space_between_bars => 1, :space_between_groups => 2}
  
  def get_system_totals list=nil#, show_units=true
    list = list || self.get_stats
    
    data = {}
    stat_types = self.get_stat_types
    list.each do |item|
      stat_types.each do |stat|
        data[:"#{stat}"] = Array.new(TIME_INTERVALS.length) unless data[:"#{stat}"]
        TIME_INTERVALS.length.times do |i|
          running_total = data[:"#{stat}"][i] || 0
          data[:"#{stat}"][i] = running_total + item.get_value(stat, i, false)
        end
        if item == list.last && self::STATS_KEY[:"#{stat}"][:units] == "%"
          TIME_INTERVALS.length.times do |i|
            data[:"#{stat}"][i] = data[:"#{stat}"][i] / list.length # this assumes there are values for each stat
            data[:"#{stat}"][i] = Table.round_to data[:"#{stat}"][i], 2
          end
        end
      end
      
    end
    [list.first.timestamps, data]
  end
  
  
  

  # find just one. this could be optimized
  def get_stat uid
    list = self.get_stats
    list.each do |item| 
      return item if item.id == uid
    end
    return nil
  end
  
  def get_stats(wait_time=2)
    list = []
    using_new_file = true

    # repeats the copy for some given time.
    time_spent = 0
    start_time = Time.now
    elapsed_time = Time.now
    begin
        elapsed_time = Time.now
        File.copy("#{self::PATH_TO_FILE}#{self::ORIGINAL_FILE_NAME}", "#{self::PATH_TO_FILE}#{self::COPY_FILE_NAME}")
    rescue => err
      time_spent = elapsed_time - start_time
      if time_spent <= wait_time
        retry
      else
        #use old file if possible
        if File.exists?("#{self::PATH_TO_FILE}#{self::COPY_FILE_NAME}")
          using_new_file = false
        else
          raise
        end
      end
    end

    begin
      #parse copied file here
      file = File.open("#{self::PATH_TO_FILE}#{self::COPY_FILE_NAME}", "r")
      current_stat = self.new
      file.each do |line|
        #start parsing...
        if line =~ /^(#{self.name.to_s}) = (.+)/
          current_stat = self.new($2)
          list.push current_stat
        elsif line =~ /^\t(.+)=(.+)/
          key = :"#{$1}"
          values = $2.split(",").map! do |v|
            if v =~ /\./
              v.to_f  #data can be floats
            else
              v.to_i
            end
          end
          
          # values = $2.split(",") #data can be floats
          if key == :Timestamps
            current_stat.timestamps = values
          else
            current_stat.data[key] = values
          end
        end
      end
      file.close
      #Uses old copied file if necessary
      # File.delete("#{self::PATH_TO_FILE}#{self::COPY_FILE_NAME}")
    rescue
      raise
    end    
    # return the array of list populated with data.
    return list
  end

  def sort(chart_key, list, sort_type, selected_stat, interval_index)
    data_type = selected_stat.to_sym
    puts data_type
    sorted = list.sort { |x, y|       
      if sort_type == "name"
        x.id <=> y.id
      elsif sort_type == "data"        
        case chart_key[:type]

        when :A
          a = y.data[chart_key[:stats][0]][interval_index]
          b = y.data[chart_key[:stats][1]][interval_index]
          
          c = x.data[chart_key[:stats][0]][interval_index]
          d = x.data[chart_key[:stats][1]][interval_index]
          #todo: handle divide by zero? doesn't blow up with 

          # special case for :disk_available
          if data_type == :disk_used
            (b - a)/(b * 1.0) <=> (d - c)/(d * 1.0)
          else
            a/(b * 1.0) <=> c/(d * 1.0)
          end

        when :B
          y.data[data_type][interval_index] <=> x.data[data_type][interval_index]
        when :C
          data_type = chart_key[:stats][0]
          y.data[data_type][interval_index] <=> x.data[data_type][interval_index]
        end
      end
    } 
    # pp sorted.map{|s| s.data[data_type]}, chart_key[:type], chart_key, sort_type, data_type, interval_index
    sorted
  end

  #todo: doesn't handle if interval_index is there. (it pushes nil)
  def get_all_stats(list, data_type, interval_index)
    data = []
    list.each do |item|
      #todo: if data doesn't exist for the selected index, push a nil value or -1? No. All values will be present
      data.push item.data[:"#{data_type}"][interval_index]
    end
    data
  end

  def pretty_titleize(title)
    t = title.to_s.titleize
    if t =~ /K Bps/
      return t.titleize.gsub!(/K Bps/,"KBps") 
    elsif t =~ /Cpu/
      return t.titleize.gsub!(/Cpu/,"CPU") 
    else
      return t.titleize
    end
  end
  
end
