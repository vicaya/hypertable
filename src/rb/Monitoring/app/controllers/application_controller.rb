# Filters added to this controller apply to all controllers in the application.
# Likewise, all the methods added will be available for all controllers.

class ApplicationController < ActionController::Base
  helper :all # include all helpers, all the time
  protect_from_forgery # See ActionController::RequestForgeryProtection for details

  # Scrub sensitive parameters from your log
  # filter_parameter_logging :password
  
  def find_largest(array)
    array.flatten.sort.last
  end

  def find_smallest(array)
    array.flatten.sort.first
  end
  
  #to use helper methods in controller
  def help
    Helper.instance
  end

  class Helper
    include Singleton
    include ActionView::Helpers::NumberHelper
  end
  
  # retrieval and graph generation from RRDtool dbs
  PATH_TO_FILE = "../../../run/monitoring/"
  VERSION_NUMBER = 0
  
  def get_all_rrd_rs_graphs range_server, stat_types
    rrd_name = range_server.name
    
    rrd = "#{PATH_TO_FILE}#{rrd_name}_stats_v#{VERSION_NUMBER}.rrd"
    
    #todo: get proper times
    # start_time = range_server.timestamps[2] / 10 ** 9
    # end_time = range_server.timestamps[0] / 10 ** 9
    now = Time.now
    end_time = now.to_i
    start_time = (now - 24.hours).to_i
    
    graphs = []
  
    stat_types.each do |stat_name|
      file_name = "#{rrd_name}_#{stat_name.to_s}"
      (fstart, fend, data) = RRD.fetch(rrd, "--start", start_time, "--end", end_time, "AVERAGE")
      graph_key = RangeServer.get_chart_type stat_name
      if RangeServer::STAT_TO_RRD_KEY[:"#{stat_name}"]     
        stat_a = RangeServer::STAT_TO_RRD_KEY[graph_key[:stats][0]]
        RRD.graph(
           "../../../src/rb/Monitoring/public/images/#{file_name}.png",
            "--title", "#{RangeServer.pretty_titleize stat_name}", 
            "--start", start_time,
            "--end", end_time,
            "DEF:#{stat_a}=#{rrd}:#{stat_a}:AVERAGE", 
            "LINE2:#{stat_a}#FF0000")
        end
        graphs.push "#{file_name}.png"
      end
      return graphs        
    end
  
end
