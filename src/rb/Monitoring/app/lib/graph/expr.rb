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

module RRD
  class Graph
    class Expr
      attr_reader :tree

      # FIXME Some functions not implemented.
      MAP = {
        :lt      => 2,
        :le      => 2,
        :gt      => 2,
        :ge      => 2,
        :eq      => 2,
        :ne      => 2,
        :un      => 1,
        :isinf   => 1,
        :if      => 3,
        :min     => 2,
        :max     => 2,
        :limit   => 3,
        :+       => 2,
        :-       => 2,
        :*       => 2,
        :/       => 2,
        :%       => 2,
        :sin     => 1,
        :cos     => 1,
        :log     => 1,
        :exp     => 1,
        :sqrt    => 1,
        :atan    => 1,
        :atan2   => 2,
        :floor   => 1,
        :ceil    => 1,
        :deg2rad => 1,
        :rad2deg => 1,
        :trend   => 2,
      }

      def self.read rrd
        new [:read, rrd]
      end

      def initialize tree
        @tree = tree
      end

      MAP.each do |func, num_args|
        meth = if func == :if then :if_ else func end

        define_method meth do |*args|
          unless args.length == num_args-1
            raise ArgumentError,
                  "wrong number of arguments (#{args.length} "\
                    "for #{num_args-1}",
                  caller
          end

          self.class.new [func, self, *args]
        end
      end

      def to_s
        @tree.to_s
      end

      def inspect
        @tree.inspect
      end
    end
  end
end

