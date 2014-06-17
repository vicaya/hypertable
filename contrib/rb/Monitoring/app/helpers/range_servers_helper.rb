module RangeServersHelper

  def link_to_if_rrd_chart_exists stat
    if RangeServer::STAT_TO_RRD_KEY[stat.to_sym]
      link_to RangeServer.pretty_titleize(stat), "##{stat}"
    else
      RangeServer.pretty_titleize(stat)
    end
  end
end
