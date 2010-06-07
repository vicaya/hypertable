class MainController < ApplicationController

  def index
    @time_intervals = FileReader::TIME_INTERVALS
    @tables = Table.get_stats
    @range_servers = RangeServer.get_stats

    @table_timestamps, @table_system_totals = Table.get_system_totals @tables
    @rs_timestamps, @rs_system_totals = RangeServer.get_system_totals @range_servers
    
    respond_to do |format|
      format.html # index.html.erb
    end
  end
end
