# -*- coding: utf-8 -*-
# Copyright © 2009 Johan Kiviniemi
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

require 'fileutils'
require 'pp'

require "#{File.dirname(__FILE__)}/graph/expr"


module RRD
  class Graph
    COLORS = ['73d216', 'ff9900', '007700', '9900ff', '666666', '000000']

    def initialize width, height, title, filename, label, interval, end_="now"
      @width    = width
      @height   = height
      @title    = title
      @filename = filename
      @label    = label
      @interval = interval.to_f
      @end_     = end_

      @params      = []
      @vars        = {}
      @var_counter = 0
      @color_i     = 0

      if block_given?
        yield self
        run
      end
    end

    def run
      tempname = @filename + '.temp'  # FIXME

      params =  %w{rrdtool graph}
      params << tempname
      params += %w{--imgformat PNG --slope-mode --interlaced}
      params += ['--end', @end_.to_s, '--start', "#{@end_}-#{@interval}"]
      params += ['--width', @width.to_s, '--height', @height.to_s]
      params += ['--title', @title.to_s, '--vertical-label', @label.to_s]
      params += %w{--color BACK#ffffff     --color CANVAS#ffffff00}
      params += %w{--color SHADEA#ffffff00 --color SHADEB#ffffff00}
      params << 'LINE1:0#0000007f'

      params += @params

      begin
        system *params

        FileUtils.mv tempname, @filename
      rescue Exception => err
        raise err
      ensure
        FileUtils.rm_f tempname
      end
    end

    def line width, expr, title
      var = process_expr expr

      color = COLORS[@color_i % COLORS.length]
      @color_i += 1

      @params << "LINE#{width}:#{var}##{color}:#{title}"
    end

    def gprint expr
      var  = process_expr expr
      min  = param "VDEF:@VAR@=#{var},MINIMUM"
      avg  = param "VDEF:@VAR@=#{var},AVERAGE"
      max  = param "VDEF:@VAR@=#{var},MAXIMUM"
      last = param "VDEF:@VAR@=#{var},LAST"
      @params << "GPRINT:#{min}:\\tmin %5.1lf %s"
      @params << "GPRINT:#{avg}:\\tavg %5.1lf %s"
      @params << "GPRINT:#{max}:\\tmax %5.1lf %s"
      @params << "GPRINT:#{last}:\\tlast %5.1lf %s\\l"
    end

    private

    def process_expr expr
      if expr.respond_to? :tree
        if expr.tree.is_a? Array
          func = expr.tree[0]
          args = expr.tree[1..-1]

          if func == :read
            param "DEF:@VAR@=#{args[0]}"

          else
            args_evaled = args.map {|a| process_expr a }

            param "CDEF:@VAR@=#{args_evaled.join(',')},#{func.to_s.upcase}"
          end

        else
          expr.tree
        end

      else
        expr
      end
    end

    def param param_expr
      if @vars.has_key? param_expr
        @vars[param_expr]

      else
        var = new_var
        param_expr_final = param_expr.sub /@VAR@/, var

        @params << param_expr_final
        @vars[param_expr] = var
      end
    end

    def new_var
      var = "var#{@var_counter}"
      @var_counter += 1
      var
    end
  end
end

