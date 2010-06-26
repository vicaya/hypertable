class MainController < ApplicationController
  def index
    @time_interval_summary = FileReader::TIME_INTERVAL_SUMMARY 
    @time_interval_tables = FileReader::TIME_INTERVAL_TABLES
    @time_interval_rs = FileReader::TIME_INTERVAL_RS
    @tables = Table.get_stats
    @range_servers = RangeServer.get_stats

    @table_timestamps, @table_system_totals = Table.get_system_totals @tables
    @rs_timestamps, @rs_system_totals = RangeServer.get_system_totals @range_servers
    
    respond_to do |format|
      format.html # index.html.erb
    end
  end
end
