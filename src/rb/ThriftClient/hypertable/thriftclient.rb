# include path hack
$:.push(File.dirname(__FILE__) + '/gen-rb')

require 'thrift'
require 'thrift/protocol/binaryprotocolaccelerated'
require 'HqlService'

module Hypertable
  class ThriftClient < ThriftGen::HqlService::Client
    def initialize(host, port, timeout_ms = 20000, do_open = true)
      @transport = Thrift::FramedTransport.new(Thrift::Socket.new(host, port))
      protocol = Thrift::BinaryProtocolAccelerated.new(@transport)
      super(protocol)
      open() if do_open
    end

    def open()
      @transport.open()
      @do_close = true
    end

    def close()
      @transport.close() if @do_close
    end

    # more convenience methods

    # buffered query
    def hql_query(hql)
      hql_exec(hql, false, false);
    end

    def with_scanner(table, scan_spec)
      scanner = open_scanner(table, scan_spec)
      yield scanner
      close_scanner(scanner)
    end

    def with_mutator(table)
      mutator = open_mutator(table);
      yield mutator
      close_mutator(mutator)
    end

    # scanner iterator
    def each_cell(scanner)
      cells = next_cells(scanner);

      while (cells.size)
        cells.each {|cell| yield cell}
        cells = next_cells(scanner);
      end
    end

  end
end
