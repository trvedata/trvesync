require 'crdt'
require 'stringio'

RSpec.describe CRDT::Encoding do

  class MockServer
    attr_reader :peers, :last_message
    FROM_CLIENT = Avro::IO::DatumReader.new(CRDT::Encoding::CLIENT_TO_SERVER_SCHEMA)
    TO_CLIENT = Avro::IO::DatumWriter.new(CRDT::Encoding::SERVER_TO_CLIENT_SCHEMA)

    def initialize(num_peers)
      peer0 = CRDT::Peer.new
      @peers = [peer0] + (1...num_peers).map {|i| CRDT::Peer.new(nil, channel_id: peer0.channel_id) }
      @offset = 0
    end

    def decode_from_client(from_client_bin)
      decoder = Avro::IO::BinaryDecoder.new(StringIO.new(from_client_bin))
      FROM_CLIENT.read(decoder)['message']
    end

    def encode_to_client(to_client)
      encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
      TO_CLIENT.write({'message' => to_client}, encoder)
      encoder.writer.string
    end

    def broadcast(sender)
      peers[sender].message_send_requests.each do |request|
        from_client = decode_from_client(request)

        @offset += 1
        to_client = {
          'channelID'   => from_client['channelID'],
          'senderID'    => [peers[sender].peer_id].pack('H*'),
          'senderSeqNo' => from_client['senderSeqNo'],
          'offset'      => @offset,
          'payload'     => from_client['payload']
        }

        to_client_bin = encode_to_client(to_client)
        peers.each {|peer| peer.receive_message(to_client_bin) }
        @last_message = CRDT::Peer::Message.new(peers[sender].peer_id, from_client['senderSeqNo'], @offset, nil, from_client['payload'])
      end
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
      expect(peer.logical_ts).to eq 3

      peer.save(@file)
      @file.rewind
      peer = CRDT::Peer.load(@file)
      expect(peer.logical_ts).to eq 3
      peer.ordered_list.insert(2, 'c')
      expect(peer.logical_ts).to eq 4
    end

    context 'message buffers' do
      it 'should save logged messages' do
        server = MockServer.new(2)
        server.peers[0].ordered_list.insert(0, 'a')
        server.broadcast(0)

        [0, 1].each do |peer_num|
          file = StringIO.new
          server.peers[peer_num].save(file)
          file.rewind
          reloaded_peer = CRDT::Peer.load(file)

          expect(reloaded_peer.message_log.size).to eq 1
          expect(reloaded_peer.message_send_requests.size).to eq 0

          logged_msg = reloaded_peer.message_log.first
          expect(logged_msg.origin_peer_id).to eq server.peers[0].peer_id
          expect(logged_msg.msg_count     ).to eq 1
          expect(logged_msg.offset        ).to eq 1
          expect(logged_msg.encoded       ).to eq server.last_message.encoded
        end
      end

      it 'should retry sending messages that were not confirmed' do
        server = MockServer.new(2)
        server.peers[0].ordered_list.insert(0, 'a')
        msg = server.decode_from_client(server.peers[0].message_send_requests.first)
        server.peers[0].save(@file)
        @file.rewind
        server.peers[0] = CRDT::Peer.load(@file)

        send_queue = server.peers[0].messages_to_send
        expect(send_queue.size).to eq 1

        logged_msg = send_queue.first
        expect(logged_msg.origin_peer_id).to eq server.peers[0].peer_id
        expect(logged_msg.msg_count     ).to eq 1
        expect(logged_msg.offset        ).to be_nil
        expect(logged_msg.encoded       ).to eq msg['payload']
      end

      it 'should retransmit messages that the server forgot about' do
        server = MockServer.new(1)
        server.peers[0].ordered_list.insert(0, 'a')
        server.broadcast(0)
        expect(server.peers[0].messages_to_send).to be_empty

        channel_id = [server.peers[0].channel_id].pack('H*')
        error = {'channelID' => channel_id, 'lastKnownSeqNo' => 0}
        server.peers[0].receive_message(server.encode_to_client(error))
        messages = server.peers[0].messages_to_send
        expect(messages.size).to eq 1
        expect(messages.first.origin_peer_id).to eq server.peers[0].peer_id
        expect(messages.first.msg_count     ).to eq 1
        expect(messages.first.encoded       ).to eq server.last_message.encoded
      end
    end
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

    it 'should log encoded messages at each peer' do
      server = MockServer.new(2)
      server.peers[0].ordered_list.insert(0, 'a')
      server.broadcast(0)

      [0, 1].each do |peer_num|
        expect(server.peers[peer_num].message_log.size).to eq 1

        logged_msg = server.peers[peer_num].message_log.first
        expect(logged_msg.origin_peer_id).to eq server.peers[0].peer_id
        expect(logged_msg.msg_count     ).to eq 1
        expect(logged_msg.offset        ).to eq 1
        expect(logged_msg.encoded       ).to eq server.last_message.encoded
      end
    end

    it 'should log messages that failed to send' do
      server = MockServer.new(2)
      server.peers[0].ordered_list.insert(0, 'a')
      server.peers[0].message_send_requests
      expect(server.peers[1].message_log.size).to eq 0
      expect(server.peers[0].message_log.size).to eq 1

      logged_msg = server.peers[0].message_log.first
      expect(logged_msg.origin_peer_id).to eq server.peers[0].peer_id
      expect(logged_msg.msg_count     ).to eq 1
      expect(logged_msg.offset        ).to be_nil
      expect(logged_msg.encoded.size  ).to be > 10
    end
  end
end
