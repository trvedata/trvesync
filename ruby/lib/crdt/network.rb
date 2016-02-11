require 'uri'
require 'thread'
require 'faye/websocket'
require 'eventmachine'

module CRDT
  # Runs a WebSocket client in a separate thread, and sends/receives messages on the WebSocket
  # connection. Automatically reconnects and resends messages if the connection is lost. Assumes
  # that the WebSocket server echoes back any messages that we send, and this echo is used as
  # confirmation that a message has been successfully delivered. Uses unbounded queues and lacks
  # backpressure, so this implementation should only be used for small data volumes that can
  # comfortably fit in memory.
  class Network
    attr_reader :logger

    RECONNECT_DELAY = 10 # seconds
    SEND_QUEUE_POLL_INTERVAL = 0.1 # seconds

    def initialize(peer, url, logger=lambda {|msg| })
      @peer = peer or raise ArgumentError, 'peer must be set'
      parsed_url = URI.parse(url)
      if !['ws', 'wss'].include?(parsed_url.scheme) || parsed_url.host.nil?
        raise ArgumentError, 'WebSocket URL should look like ws://host[:port]/path'
      end
      parsed_url.query = URI.encode_www_form(URI.decode_www_form(parsed_url.query || '') + [['peer_id', peer.peer_id]])
      @url = parsed_url.to_s
      @logger = logger
      @subscribe_request = peer.encode_subscribe_request
      @channel_id = peer.channel_id
      @recv_queue = Queue.new
      @send_queue = Queue.new
      @in_flight = []
      @retry_queue = []
    end

    def run
      Thread.new do
        EventMachine.run do
          EventMachine.add_periodic_timer(SEND_QUEUE_POLL_INTERVAL, &method(:send_queue_poll))
          connect
        end
      end
    end

    # Called periodically by the editor thread to process any incoming and outgoing messages in the
    # peer. We never call the peer directly from the EventMachine thread, to avoid having to worry
    # about thread safety on the peer.
    def poll
      @send_queue << @peer.encode_message if @peer.anything_to_send?
      while !@recv_queue.empty?
        @peer.receive_message(@recv_queue.pop(true))
      end
    end

    private

    def connect
      logger.call "Connecting to WebSocket server at #@url on channel #@channel_id"
      websocket = Faye::WebSocket::Client.new(@url, nil, :ping => 20)

      websocket.on :open do |event|
        raise 'Two connections open simultaneously' if @websocket
        @websocket = websocket
        logger.call 'Connected to WebSocket server'
        @websocket.send(@subscribe_request.unpack('C*'))
        send_queue_poll
      end

      websocket.on :message, &method(:receive_message)

      websocket.on :error do |event|
        logger.call "WebSocket connection error: #{event.message}"
      end

      websocket.on :close do |event|
        logger.call "Connection to WebSocket server closed: code=#{event.code} reason=#{event.reason}"
        @websocket = nil
        @retry_queue.unshift(*@in_flight)
        @in_flight.clear
        EventMachine.add_timer(RECONNECT_DELAY) { connect }
      end
    end

    # Called periodically by EventMachine reactor thread to send any messages
    # waiting in the send queue.
    def send_queue_poll
      send_message(@retry_queue.shift)    while @websocket && !@retry_queue.empty?
      send_message(@send_queue.pop(true)) while @websocket && !@send_queue.empty?
    end

    def send_message(message)
      @in_flight << message

      # The send queue contains messages as binary-encoded strings. To tell the WebSocket library to
      # send the message as binary, convert it into an array of numbers, where each number corresponds
      # to one byte of the message string.
      @websocket.send(message.unpack('C*'))
    end

    # Called by EventMachine reactor thread when a message is received from the WebSocket server.
    def receive_message(event)
      message = event.data.is_a?(Array) ? event.data.pack('C*') : event.data
      # TODO need to handle messages getting reordered?
      @in_flight.shift if @in_flight.first == message
      @recv_queue << message
    end
  end
end
