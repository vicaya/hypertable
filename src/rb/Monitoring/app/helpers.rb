Dir["#{ File.dirname(__FILE__)}/helpers/*.rb"].each{ |r| require r}

module HTMonitoring
  module Helpers
    include Util
  end
end
