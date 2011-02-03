#!/usr/bin/env ruby
require 'rubygems'
require 'yaml'
require 'json'
require 'titleize'
require 'sinatra/base'
require 'pathname'

%w(helpers).each {  |r| require "#{  File.dirname(__FILE__)}/app/#{r}"}
Dir["#{  File.dirname(__FILE__)}/app/lib/*.rb"].each {|r| require r}

module HTMonitoring
  @root = Pathname.new(File.dirname(__FILE__)).expand_path
  @hypertable_home = ENV['HYPERTABLE_HOME']

  def self.config
    @config ||= YAML.load_file(@root.join("app/config/config.yml"))[:production]
    @config[:data] = eval "\"#{@config[:data]}\""
    @config
  end

  def self.tablestats
    @tablestats ||= YAML.load_file(@root.join("app/config/tablestats.yml"))[:stats]
  end

  def self.rangeserverstats
    @rangeserverstats ||= YAML.load_file(@root.join("app/config/rangeserverstats.yml"))[:stats]
  end

  def self.rrdstats
    @rrdstats ||= YAML.load_file(@root.join("app/config/rrdstats.yml"))[:stats]
  end

  class Admin < Sinatra::Base
    @root = Pathname.new(File.dirname(__FILE__)).expand_path
    set :environment, :production
    set :public, @root.join('app/public')
    set :views,  @root.join('app/views')
    set :logging, true

    enable :static
    disable :run

    helpers do
      include HTMonitoring::Helpers
    end

    get '/' do
      @range_servers = StatsJson.new(:file => 'rangeserver_summary.json')
      @rs_records = @range_servers.parse_stats_file
      erb :index
    end

    get '/tables' do
      @tables = StatsJson.new(:file => 'table_summary.json')
      @table_records = @tables.parse_stats_file
      erb :tables
    end

    get '/graphs' do
      @server = params[:server]
      @stype = params[:type]
      if @stype.nil?
        @stype = "RangeServer"
      end
      erb :graphs
    end

    error do
      erb :error
    end

    get '/data/:server/:key/:type' do
      if params[:server].downcase == "rangeserver" and params[:key].downcase == "servers"
        rrd_stats = RRDStat.new
        json = rrd_stats.get_server_list
        graph_callback(json)
      elsif params[:server].downcase == "table"
        rrd_stats = RRDStat.new
        json = rrd_stats.get_table_list
        graph_callback(json)
      end
    end

    get '/graph/:type/:server/:stat/:starttime/:endtime' do
      content_type 'image/gif'
      if params[:type].downcase == "rangeserver"
        rrd_stats = RRDStat.new
        image_data = rrd_stats.get_rrd_stat_image params[:server],params[:stat],params[:starttime],params[:endtime]
      elsif params[:type].downcase == "table"
        rrd_stats = RRDStat.new
        image_data = rrd_stats.get_table_stat_image params[:server],params[:stat],params[:starttime],params[:endtime]
      end
    end

     get '/data/:type/:stat/:timestamp_index/:sort_by/:resolution' do
      if params[:type].downcase == "table"
        stats = TableStats.new
        json = stats.get_graph_data({:stat => params[:stat], :timestamp_index => params[:timestamp_index].to_i, :sort_by => params[:sort_by]})
        graph_callback(json)
      elsif params[:type].downcase == "rangeserver"
        stats = RRDStat.new
        json = stats.get_graph_data({:stat => params[:stat], :timestamp_index => params[:timestamp_index].to_i, :sort_by => params[:sort_by],:resolution => params[:resolution]})
        graph_callback(json)
      end
    end

    def graph_callback(json)
      params[:callback] ? params[:callback] + '(' + json + ')' : json
    end

  end

end
