require 'crdt'
require 'tempfile'

RSpec.describe CRDT::Encoding do
  def new_tempfile
    Tempfile.new('crdt_peer', Dir.tmpdir, 'wb').tap do |file|
      @tempfiles << file
    end
  end

  def decode_msg(serialized)
    decoder = Avro::IO::BinaryDecoder.new(StringIO.new(serialized))
    reader = Avro::IO::DatumReader.new(CRDT::Encoding::MESSAGE_SCHEMA)
    reader.read(decoder)
  end

  context 'saving peer state' do
    before(:each) { @tempfiles = [] }
    after (:each) { @tempfiles.each(&:close).each(&:unlink) }

    it 'should save and reload an empty peer' do
      peer = CRDT::Peer.new
      peer.save(new_tempfile)
      reloaded = CRDT::Peer.load(@tempfiles.first.path)
      expect(reloaded.peer_id).to eq peer.peer_id
    end

    it 'should save and reload data structure contents' do
      peer = CRDT::Peer.new
      peer.ordered_list.insert(0, 'a').insert(1, 'b').insert(2, 'c').insert(3, 'd').delete(0)
      expect(peer.ordered_list.to_a.join).to eq 'bcd'

      peer.save(new_tempfile)
      reloaded = CRDT::Peer.load(@tempfiles.first.path)
      expect(reloaded.ordered_list.to_a.join).to eq 'bcd'
    end

    it 'should preserve tombstones' do
      peer1, peer2 = CRDT::Peer.new, CRDT::Peer.new
      peer1.ordered_list.insert(0, 'a').insert(1, 'b')
      peer2.receive_message(peer1.encode_message)
      peer2.ordered_list.insert(2, 'c')
      peer1.ordered_list.delete(1)
      peer2.receive_message(peer1.encode_message)
      expect(peer2.ordered_list.to_a).to eq ['a', 'c']

      peer1.save(new_tempfile)
      peer1 = CRDT::Peer.load(@tempfiles.first.path)
      peer1.receive_message(peer2.encode_message)
      expect(peer1.ordered_list.to_a).to eq ['a', 'c']
    end

    it 'should save and reload the peer matrix' do
      peer1, peer2 = CRDT::Peer.new, CRDT::Peer.new
      peer1.ordered_list.insert(0, 'a')
      peer2.receive_message(peer1.encode_message)
      expect(peer2.peer_matrix.peer_id_to_index(peer2.peer_id)).to eq 0
      expect(peer2.peer_matrix.peer_id_to_index(peer1.peer_id)).to eq 1

      peer2.save(new_tempfile)
      peer2 = CRDT::Peer.load(@tempfiles.first.path)

      expect(peer2.peer_matrix.peer_id_to_index(peer2.peer_id)).to eq 0
      expect(peer2.peer_matrix.peer_id_to_index(peer1.peer_id)).to eq 1
    end

    it 'should save and reload the Lamport clock' do
      peer = CRDT::Peer.new
      peer.ordered_list.insert(0, 'a').insert(1, 'b')
      expect(peer.logical_ts).to eq 2

      peer.save(new_tempfile)
      peer = CRDT::Peer.load(@tempfiles.first.path)
      expect(peer.logical_ts).to eq 2
      peer.ordered_list.insert(2, 'c')
      expect(peer.logical_ts).to eq 3
    end

    it 'should save and reload message buffers'
  end

  context 'sending and receiving messages' do
    it 'should encode CRDT operations' do
      peer1, peer2 = CRDT::Peer.new, CRDT::Peer.new
      peer1.ordered_list.insert(0, 'a').insert(1, 'b').delete(0)
      peer2.receive_message(peer1.encode_message)
      expect(peer2.ordered_list.to_a).to eq ['b']
    end

    it 'should track causal dependencies' do
      peer1, peer2, peer3 = CRDT::Peer.new, CRDT::Peer.new, CRDT::Peer.new
      peer1.ordered_list.insert(0, 'a')
      msg1 = peer1.encode_message

      peer2.receive_message(msg1)
      peer2.ordered_list.insert(1, 'b')
      msg2 = peer2.encode_message

      peer3.receive_message(msg2)
      expect(peer3.ordered_list.to_a).to eq []
      peer3.receive_message(msg1)
      expect(peer3.ordered_list.to_a).to eq ['a', 'b']
    end

    it 'should handle concurrent operations' do
      peer1, peer2 = CRDT::Peer.new, CRDT::Peer.new
      peer1.ordered_list.insert(0, 'b')
      peer2.receive_message(peer1.encode_message)

      peer1.ordered_list.insert(0, 'a')
      peer2.ordered_list.insert(1, 'c')
      peer2.receive_message(peer1.encode_message)
      peer1.receive_message(peer2.encode_message)

      expect(peer1.ordered_list.to_a).to eq ['a', 'b', 'c']
      expect(peer2.ordered_list.to_a).to eq ['a', 'b', 'c']
    end
  end
end
