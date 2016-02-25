require 'crdt'
require 'stringio'

RSpec.describe CRDT::Encoding do

  class MockServer
    attr_reader :channel_id, :peers
    FROM_CLIENT = Avro::IO::DatumReader.new(CRDT::Encoding::CLIENT_TO_SERVER_SCHEMA)
    TO_CLIENT = Avro::IO::DatumWriter.new(CRDT::Encoding::SERVER_TO_CLIENT_SCHEMA)

    def initialize(num_peers)
      @channel_id = OpenSSL::Random.random_bytes(16).unpack('H*').first
      @peers = (0...num_peers).map { CRDT::Peer.new(nil, channel_id: @channel_id) }
      @offset = 0
    end

    def broadcast(sender)
      from_client_bin = peers[sender].encode_message
      decoder = Avro::IO::BinaryDecoder.new(StringIO.new(from_client_bin))
      from_client = FROM_CLIENT.read(decoder)['message']

      @offset += 1
      to_client = {
        'channelID'   => from_client['channelID'],
        'senderID'    => [peers[sender].peer_id].pack('H*'),
        'senderSeqNo' => from_client['senderSeqNo'],
        'offset'      => @offset,
        'payload'     => from_client['payload']
      }

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
      TO_CLIENT.write({'message' => to_client}, encoder)
      to_client_bin = encoder.writer.string
      peers.each {|peer| peer.receive_message(to_client_bin) }
    end
  end

  context 'saving peer state' do
    before(:each) do
      @file = StringIO.new
    end

    it 'should save and reload an empty peer' do
      peer = CRDT::Peer.new
      peer.save(@file)
      @file.rewind
      reloaded = CRDT::Peer.load(@file)
      expect(reloaded.peer_id).to eq peer.peer_id
    end

    it 'should save and reload data structure contents' do
      peer = CRDT::Peer.new
      peer.ordered_list.insert(0, 'a').insert(1, 'b').insert(2, 'c').insert(3, 'd').delete(0)
      expect(peer.ordered_list.to_a.join).to eq 'bcd'

      peer.save(@file)
      @file.rewind
      reloaded = CRDT::Peer.load(@file)
      expect(reloaded.ordered_list.to_a.join).to eq 'bcd'
    end

    it 'should preserve tombstones' do
      server = MockServer.new(2)
      server.peers[0].ordered_list.insert(0, 'a').insert(1, 'b')
      server.broadcast(0)
      server.peers[1].ordered_list.insert(2, 'c')
      server.peers[0].ordered_list.delete(1)
      server.broadcast(0)
      expect(server.peers[1].ordered_list.to_a).to eq ['a', 'c']

      server.peers[0].save(@file)
      @file.rewind
      server.peers[0] = CRDT::Peer.load(@file)
      server.broadcast(1)
      expect(server.peers[0].ordered_list.to_a).to eq ['a', 'c']
    end

    it 'should save and reload the peer matrix' do
      server = MockServer.new(2)
      server.peers[0].ordered_list.insert(0, 'a')
      server.broadcast(0)
      expect(server.peers[1].peer_matrix.peer_id_to_index(server.peers[1].peer_id)).to eq 0
      expect(server.peers[1].peer_matrix.peer_id_to_index(server.peers[0].peer_id)).to eq 1

      server.peers[1].save(@file)
      @file.rewind
      server.peers[1] = CRDT::Peer.load(@file)

      expect(server.peers[1].peer_matrix.peer_id_to_index(server.peers[1].peer_id)).to eq 0
      expect(server.peers[1].peer_matrix.peer_id_to_index(server.peers[0].peer_id)).to eq 1
    end

    it 'should save and reload the Lamport clock' do
      peer = CRDT::Peer.new
      peer.ordered_list.insert(0, 'a').insert(1, 'b')
      expect(peer.logical_ts).to eq 2

      peer.save(@file)
      @file.rewind
      peer = CRDT::Peer.load(@file)
      expect(peer.logical_ts).to eq 2
      peer.ordered_list.insert(2, 'c')
      expect(peer.logical_ts).to eq 3
    end

    it 'should save and reload message buffers'
  end

  context 'sending and receiving messages' do
    it 'should encode CRDT operations' do
      server = MockServer.new(2)
      server.peers[0].ordered_list.insert(0, 'a').insert(1, 'b').delete(0)
      server.broadcast(0)
      expect(server.peers[1].ordered_list.to_a).to eq ['b']
    end

    it 'should track causal dependencies' do
      server = MockServer.new(3)
      server.peers[0].ordered_list.insert(0, 'a')
      server.broadcast(0)
      server.peers[1].ordered_list.insert(1, 'b')
      server.broadcast(1)
      expect(server.peers[2].ordered_list.to_a).to eq ['a', 'b']
    end

    it 'should handle concurrent operations' do
      server = MockServer.new(2)
      server.peers[0].ordered_list.insert(0, 'b')
      server.broadcast(0)

      server.peers[0].ordered_list.insert(0, 'a')
      server.peers[1].ordered_list.insert(1, 'c')
      server.broadcast(0)
      server.broadcast(1)

      expect(server.peers[0].ordered_list.to_a).to eq ['a', 'b', 'c']
      expect(server.peers[1].ordered_list.to_a).to eq ['a', 'b', 'c']
    end
  end
end
