# include path hack
$:.push(File.dirname(__FILE__) + '/gen-rb')

require 'thrift'
require 'thrift/protocol/binaryprotocolaccelerated'
require 'HqlService'

module Hypertable
  class ThriftClient < ThriftGen::HqlService::Client
    def initialize(host, port, timeout_ms = 20000, do_open = true)
      socket = Thrift::Socket.new(host, port, timeout_ms)
      @transport = Thrift::FramedTransport.new(socket)
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

    def with_scanner(table, scan_spec)
      scanner = open_scanner(table, scan_spec)
      begin
        yield scanner
      ensure
        close_scanner(scanner)
      end
    end

    def with_mutator(table)
      mutator = open_mutator(table);
      begin
        yield mutator
      ensure
        close_mutator(mutator, 1)
      end
    end

    # scanner iterator
    def each_cell(scanner)
      cells = next_cells(scanner);

      while (cells.size > 0)
        cells.each {|cell| yield cell}
        cells = next_cells(scanner);
      end
    end
  end

  def self.with_thrift_client(host, port, timeout_ms = 20000)
    client = ThriftClient.new(host, port, timeout_ms)
    begin
      yield client
    ensure
      client.close()
    end
  end
end
