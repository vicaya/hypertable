class TablesController < ApplicationController
  include GoogleChart
  
  def index
    @time_intervals = FileReader::TIME_INTERVALS
    tables = Table.get_stats

    @stat_types = Table.get_stat_types
    @sort_types = ["data", "name"] 
    
    @selected_sort = params[:sort_by] || @sort_types[0] # default if no params in url
    @selected_stat = params[:data_type] || @stat_types[0]
    @timestamp_index = params[:time_interval].blank? ? 2 : params[:time_interval].to_i # default interval at index 2 (10 minutes has interesting test data)
    
    @chart_type = Table.get_chart_type @selected_stat
    sorted_tables = Table.sort(@chart_type, tables, @selected_sort, @selected_stat, @timestamp_index)

    #todo: handle large charts better. currently will only show top ones (that fit in 1 graph)
    max_size = max_elements_in_chart(@chart_type)
    
    #temp, throws away elements that won't fit on the graph
    sorted_tables = sorted_tables.slice(0..(max_size - 1))
    
    # dynamic charts
    @chart = generate_chart(@chart_type, sorted_tables, @selected_sort, @timestamp_index, @selected_stat)

    @json_map = json_map(@chart)
    @html_map = generate_html_map(@json_map, sorted_tables, @selected_stat, @timestamp_index)    
    
    #todo: this selects the first table's timestamp.
    @time = Time.at sorted_tables.first.timestamps[@timestamp_index] / 10 ** 9
    
    respond_to do |format|
      format.html # index.html.erb
    end
  end
  
  def show
    @time_intervals = FileReader::TIME_INTERVALS
    @table = Table.get_stat params[:id]
    @stat_types = Table.get_stat_types #array of symbols    
  end

end
