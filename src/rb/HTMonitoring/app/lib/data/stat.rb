# This class represent  data in stats file
# Each table or range server has an id and several stats for different time stamps
# id represents table or rangeserver id in the stats file
# stats is a hash collecting data for each column

class Stat
  attr_accessor :id,:stats

  def initialize(id)
    @id = id
    @stats = Array.new
  end

  def get_timestamps
    timestamps = []
    @stats.each do |stat|
      timestamps.push stat[:Timestamps]
    end
    return timestamps
  end

end
