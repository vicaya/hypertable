# Methods added to this helper will be available to all templates in the application.
module ApplicationHelper
  include GoogleChart
  
  def sort_data_hash(data)
    data.sort {|a,b| a.to_s <=> b.to_s }
  end
  
end
