module HTMonitoring
  module Helpers
    module Util

      def pretty_title(title)
        t = title.to_s.titleize
        if t =~ /K Bps/
          return t.titleize.gsub!(/K Bps/,"KBps")
        elsif t =~ /Cpu/
          return t.titleize.gsub!(/Cpu/,"CPU")
        else
          return t.titleize
        end
      end

      def number_with_delimiter(number,delimiter=',')
        begin
          if number != 0
            parts = number.to_s
            parts.gsub!(/(\d)(?=(\d\d\d)+(?!\d))/, "\\1#{delimiter}")
            return parts
          else
            return number
          end
        rescue
          number
        end
      end

      def sort_data_hash(data)
        data.sort { |a,b| a.to_s <=> b.to_s }
      end

      def find_largest(array)
        array.flatten.sort.last
      end

      def find_smallest(array)
        array.flatten.sort.first
      end


      def url_for url_fragment, mode=:path_only
        case mode
        when :path_only
          base = request.script_name
        when :full
          scheme = request.scheme
          if (scheme == 'http' && request.port == 80 ||
              scheme == 'https' && request.port == 443)
            port = ""
          else
            port = ":#{request.port}"
          end
          base = "#{scheme}://#{request.host}#{port}#{request.script_name}"
        else
          raise TypeError, "Unknown url_for mode #{mode}"
        end
        "#{base}#{url_fragment}"
      end
    end

    def image_tag(source, options = { })
      options[:src] = source
      tag("img", options)
    end

    def tag(name, local_options = { })
      start_tag = "<#{name}#{tag_options(local_options) if local_options}"
      if block_given?
        content = yield
        return "#{name}>#{content}</#{name}>"
      end
    end

    def tag_options(options)
      unless options.empty?
        attrs = []
        attrs = options.map {  |key, value| %(#{ key}="#{ Rack::Utils.escape_html(value)}") }
        " #{ attrs.sort * ' '}" unless attrs.empty?
      end

    end

    def nav_tab_on(page)
      if request.path_info == "/" and page == "index"
        return "on"
      elsif request.path_info == "/tables" and page == "tables"
        return "on"
      elsif request.path_info == "/rangeservers" and page == "rangeservers"
        return "on"
      end
      return "off"
    end
  end
end
