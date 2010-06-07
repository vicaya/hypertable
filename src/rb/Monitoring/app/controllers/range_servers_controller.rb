class RangeServersController < ApplicationController
  include GoogleChart
  
  def index
    @time_intervals = FileReader::TIME_INTERVALS
    range_servers = RangeServer.get_stats

    @stat_types = RangeServer.get_stat_types
    pp @stat_types
    
    @sort_types = ["data", "name"] 
    
    @selected_sort = params[:sort_by] || @sort_types[0] # default if no params in url
    @selected_stat = params[:data_type] || @stat_types[0]
    @timestamp_index = params[:time_interval].blank? ? 2 : params[:time_interval].to_i # default interval at index 2 (10 minutes has interesting test data)
    
    @chart_type = RangeServer.get_chart_type @selected_stat
    sorted_range_servers = RangeServer.sort(@chart_type, range_servers, @selected_sort, @selected_stat, @timestamp_index)
    
    #todo: handle large charts better. currently will only show top ones (that fit in 1 graph)
    max_size = max_elements_in_chart(@chart_type)
    
    #temp, throws away elements that won't fit on the graph
    sorted_tables = sorted_range_servers.slice(0..(max_size - 1))

    #todo: give unique ids when creating multiple graphs on one page
    @graph_id = 0 
    
    # stats_array = RangeServer.get_all_stats(sorted_range_servers, @selected_stat, @timestamp_index)
    @chart = generate_chart(@chart_type, sorted_range_servers, @selected_sort, @timestamp_index, @selected_stat)
    
    @json_map = json_map(@chart)    
    @html_map = generate_html_map(@json_map, sorted_range_servers, @selected_stat, @timestamp_index)
    
    #todo: this selects the first table's timestamp.
    @time = Time.at sorted_range_servers.first.timestamps[@timestamp_index] / 10 ** 9
    
    # if request is ajax render only 
    
    respond_to do |format|
      format.html # index.html.erb
      format.js {render :partial => "ajax_data"}
    end
  end
  
  def show
    @time_intervals = FileReader::TIME_INTERVALS
    @range_server = RangeServer.get_stat params[:id]
    @stat_types = RangeServer.get_stat_types #array of symbols
    
    @rs_rrd_graphs = get_all_rrd_rs_graphs @range_server, @stat_types
  end
    
end
