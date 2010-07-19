#!/usr/bin/env ruby
require 'rubygems'
require 'yaml'
require 'json'
require 'titleize'
require 'sinatra/base'

%w(helpers).each {  |r| require "#{  File.dirname(__FILE__)}/app/#{r}"}
#Dir["app/lib/*.rb"].each {|r| require r}
Dir["#{  File.dirname(__FILE__)}/app/lib/data/*.rb"].each {|r| require r}

require 'titleize'
require 'pathname'

module HTMonitoring
  @root = Pathname.new(File.dirname(__FILE__)).expand_path

  def self.config
    @config ||= YAML.load_file(@root.join("app/config/config.yml"))[:production]
  end

  def self.tablestats
    @tablestats ||= YAML.load_file(@root.join("app/config/tablestats.yml"))[:stats]
  end

  def self.rangeserverstats
    @rangeserverstats ||= YAML.load_file(@root.join("app/config/rangeserverstats.yml"))[:stats]
  end

  class Admin < Sinatra::Base
    @root = Pathname.new(File.dirname(__FILE__)).expand_path
    set :environment, :production
    set :public, @root.join('app/public')
    set :views,  @root.join('app/views')

    enable :static
    disable :run

    helpers do
      include HTMonitoring::Helpers
    end

    get '/' do
      @table_stats = TableStats.new
      @table_stats.get_stats_totals

      @range_servers = RangeServerStats.new
      @range_servers.get_stats_totals

      erb :index
    end

    get '/tables' do
      erb :tables
    end

    get '/rangeservers' do
      erb :rangeservers
    end

    error do
       request.env['sinatra.error'].message
    end

    get %r{/data/([^/]+)/([^/]+)/([^/]+)} do
      type = params[:captures][0]
      stat = params[:captures][1]
      time_interval = params[:captures][2]
      if type.downcase == "table"
        stats = TableStats.new
        json = stats.get_graph_data({:stat => stat, :timestamp_index => time_interval.to_i})
        graph_callback(json)
      elsif type.downcase == "rangeserver"
        stats = RRDStat.new
        json = stats.get_graph_data({:stat => stat, :timestamp_index => time_interval.to_i})
        graph_callback(json)
      end
    end

    def graph_callback(json)
      params[:callback] ? params[:callback] + '(' + json + ')' : json
    end

  end

end
