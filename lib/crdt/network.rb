require 'socket'
require 'thread'

module CRDT
  # Runs a TCP client or TCP server in a separate thread, and queues up any incoming data. The main
  # application thread needs to poll for new data, so that it is applied to the peer in the main
  # thread.
  class Network
    def initialize(peer, options={})
      @peer = peer or raise ArgumentError, 'peer must be set'
      @recv_queue = Queue.new
      @send_queue = []

      if options[:server].is_a? String
        _, host, port = options[:server].match(/\A(?:(.*):)?(\d+)\z/).to_a
        raise ArgumentError, 'Server must be specified as host:port or just port' if port.nil?
        @server = TCPServer.new(host, port.to_i)
      end

      if options[:client].is_a? String
        raise ArgumentError, 'Cannot be both a server and a client' if @server
        _, host, port = options[:client].match(/\A(.*):(\d+)\z/).to_a
        raise ArgumentError, 'Client must be specified as host:port' if host.nil? || port.nil?
        @connection = TCPSocket.new(host, port.to_i)
      end
    end

    def run
      Thread.new do
        if @server
          server_run
        elsif @connection
          client_run
        end
      end
    end

    def poll
      send_message(@peer.encode_message) if @peer.anything_to_send?
      while !@recv_queue.empty?
        @peer.receive_message(@recv_queue.pop(true))
      end
    end

    private

    # Very simplistic, only handles a single client!
    def server_run
      @connection = @server.accept
      @send_queue.each {|data| @connection.write(data) }
      @send_queue = []
      client_run
    end

    def client_run
      loop do
        length_bin = @connection.read(4)
        break if length_bin.nil? || length_bin.bytesize < 4
        length = length_bin.unpack('N').first
        @recv_queue << @connection.read(length)
      end
    end

    def send_message(bytes)
      with_length = [bytes.bytesize].pack('N') + bytes
      if @connection
        @connection.write(with_length)
      else
        @send_queue << with_length
      end
    end
  end
end
