require 'crdt'
require 'crdt/network'
require 'rspec/mocks/standalone'

RSpec.describe CRDT::Network do
  # Implements the interface of CRDT::Peer.
  class MockPeer
    def peer_id; '0000'; end
    def channel_id; '1234'; end
    def encode_subscribe_request; 'subscribe'; end

    def message_send_requests
      return [] if @to_send.nil?
      [@to_send].tap { @to_send = nil }
    end

    def send_message(message)
      @to_send = message
    end

    def receive_message(message)
      received << message
    end

    def received
      @received ||= []
    end
  end

  # Implements the interface of Faye::WebSocket::Client.
  class MockClient
    include RSpec::Mocks::ExampleMethods

    def initialize(*args)
      MockClient.instances << self
    end

    def self.instances
      @instances ||= []
    end

    def on(event, &handler)
      @events ||= {}
      @events[event] = handler
    end

    def trigger_open
      @events[:open].call(double('open_event'))
    end

    def trigger_message(message)
      @events[:message].call(double('message_event', data: message))
    end

    def trigger_error(message)
      @events[:error].call(double('error_event', message: message))
    end

    def trigger_close(code, reason)
      @events[:close].call(double('close_event', code: code, reason: reason))
    end

    def send(message)
      messages << message
    end

    def messages
      @messages ||= []
    end
  end

  before :each do
    @em = double('EventMachine')
    stub_const('EventMachine', @em)
    stub_const('Faye::WebSocket::Client', MockClient)
    MockClient.instances.clear
    @peer = MockPeer.new
    @net = CRDT::Network.new(@peer, 'ws://server.example.com/events')
    @net.send(:connect)
    @client = MockClient.instances.first
  end

  it 'should reconnect automatically' do
    @net.send(:send_queue_poll)
    expect(MockClient.instances.size).to eq 1
    @client.trigger_open
    @net.send(:send_queue_poll)

    expect(@em).to receive(:add_timer) do |timeout, &block|
      expect(timeout).to eq CRDT::Network::RECONNECT_DELAY
      @reconnect = block
    end
    @client.trigger_close(1006, '')
    @net.send(:send_queue_poll)

    @reconnect.call
    expect(MockClient.instances.size).to eq 2
  end

  it 'should send messages when connected to the server' do
    @client.trigger_open
    @peer.send_message('hello')
    @net.poll
    @net.send(:send_queue_poll)
    expect(@client.messages).to eq ['subscribe'.unpack('C*'), 'hello'.unpack('C*')]
  end

  it 'should flush queued messages when a connection is established' do
    @peer.send_message('one'); @net.poll
    @net.send(:send_queue_poll)
    @peer.send_message('two'); @net.poll
    @net.send(:send_queue_poll)
    expect(@client.messages).to eq []
    @client.trigger_open
    expect(@client.messages).to eq ['subscribe'.unpack('C*'), 'one'.unpack('C*'), 'two'.unpack('C*')]
  end

  it 'should receive messages sent by the server' do
    @client.trigger_open
    @client.trigger_message('from the server'.unpack('C*'))
    @client.trigger_message('message 2'.unpack('C*'))
    @net.poll
    expect(@peer.received).to eq ['from the server', 'message 2']
  end

  it 'should resend unconfirmed messages when reconnecting' do
    @client.trigger_open
    @peer.send_message('hello'); @net.poll
    @peer.send_message('again'); @net.poll
    @net.send(:send_queue_poll)
    expect(@client.messages).to eq ['subscribe'.unpack('C*'), 'hello'.unpack('C*'), 'again'.unpack('C*')]

    expect(@em).to receive(:add_timer) {|_, &block| @reconnect = block }
    @client.trigger_close(1006, '')
    @reconnect.call
    client2 = MockClient.instances.last
    expect(client2.messages).to be_empty

    client2.trigger_open
    expect(client2.messages).to eq ['subscribe'.unpack('C*'), 'hello'.unpack('C*'), 'again'.unpack('C*')]
  end

  it 'should not resend messages that have been confirmed by the server' do
    @peer.send_message('hello'); @net.poll
    @peer.send_message('again'); @net.poll
    @client.trigger_open
    expect(@client.messages).to eq ['subscribe'.unpack('C*'), 'hello'.unpack('C*'), 'again'.unpack('C*')]

    @client.trigger_message('hello'.unpack('C*'))

    expect(@em).to receive(:add_timer) {|_, &block| @reconnect = block }
    @client.trigger_close(1006, '')
    @reconnect.call
    client2 = MockClient.instances.last

    client2.trigger_open
    expect(client2.messages).to eq ['subscribe'.unpack('C*'), 'again'.unpack('C*')]
  end
end
