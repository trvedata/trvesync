require 'crdt'

RSpec.describe CRDT::Map do

  # make_peers(n) creates n peers on the same channel
  def make_peers(num_peers)
    peer0 = CRDT::Peer.new('peer0')
    [peer0] + (1...num_peers).map do |i|
      CRDT::Peer.new("peer#{i}", channel_id: peer0.channel_id)
    end
  end

  context 'in local operation' do
    it 'should be empty by default' do
      peer = CRDT::Peer.new(:peer1)
      expect(peer.cursors.to_a).to eq []
      expect(peer.cursors['hello']).to be_nil
    end

    it 'should contain any inserted items' do
      peer = CRDT::Peer.new(:peer1)
      peer.cursors['foo'] = 'bar'
      peer.cursors['one'] = 'two'
      expect(peer.cursors.to_a.sort).to eq [['foo', 'bar'], ['one', 'two']]
      expect(peer.cursors['foo']).to eq 'bar'
    end

    it 'should contain the latest value for a key' do
      peer = CRDT::Peer.new(:peer1)
      peer.cursors['key'] = 'first'
      peer.cursors['key'] = 'second'
      expect(peer.cursors.to_a).to eq [['key', 'second']]
      expect(peer.cursors['key']).to eq 'second'
    end
  end

  context 'generating operations' do
    it 'should include details of a put operation' do
      peer = CRDT::Peer.new(:peer1)
      peer.make_message
      peer.cursors['foo'] = 'bar'
      expect(peer.make_message.operations).to eq [
        CRDT::Map::PutOp.new(
          CRDT::OperationHeader.new(CRDT::ItemID.new(2, :peer1), peer.default_schema_id, nil, [0]),
          'foo', 'bar')
      ]
    end

    it 'should use a write operation for updates to an existing key' do
      peer = CRDT::Peer.new(:peer1)
      peer.cursors['foo'] = 'first'
      peer.make_message
      peer.cursors['foo'] = 'second'
      expect(peer.make_message.operations).to eq [
        CRDT::Map::WriteOp.new(
          CRDT::OperationHeader.new(
            CRDT::ItemID.new(3, :peer1),
            peer.default_schema_id,
            CRDT::ItemID.new(2, :peer1),
            []),
          'second')
      ]
    end
  end

  context 'applying remote operations' do
    it 'should apply changes from another peer' do
      peer1, peer2 = make_peers(2)
      peer1.cursors['foo'] = 'bar'
      peer2.process_message(peer1.make_message)
      expect(peer2.cursors.to_a).to eq [['foo', 'bar']]
    end

    it 'should merge concurrent changes to different keys' do
      peer1, peer2 = make_peers(2)
      peer1.cursors['one'] = 'first'
      peer2.cursors['two'] = 'second'
      peer2.process_message(peer1.make_message)
      peer1.process_message(peer2.make_message)
      expect(peer1.cursors.to_a.sort).to eq [['one', 'first'], ['two', 'second']]
      expect(peer2.cursors.to_a.sort).to eq [['one', 'first'], ['two', 'second']]
    end

    it 'should deterministically resolve concurrent puts of the same key' do
      peer1, peer2 = make_peers(2)
      peer1.cursors['key'] = 'first'
      peer2.cursors['key'] = 'second'
      peer2.process_message(peer1.make_message)
      peer1.process_message(peer2.make_message)
      expect(peer1.cursors.to_a.sort).to eq [['key', 'first']]
      expect(peer2.cursors.to_a.sort).to eq [['key', 'first']]
    end

    it 'should allow another peer to overwrite a value' do
      peer1, peer2 = make_peers(2)
      peer1.cursors['key'] = 'first'
      peer2.process_message(peer1.make_message)
      peer2.cursors['key'] = 'second'
      peer1.process_message(peer2.make_message)
      expect(peer1.cursors.to_a.sort).to eq [['key', 'second']]
      expect(peer2.cursors.to_a.sort).to eq [['key', 'second']]
    end

    it 'should deterministically resolve concurrent writes to the same register' do
      peer1, peer2 = make_peers(2)
      peer1.cursors['key'] = 'first'
      peer2.process_message(peer1.make_message)
      peer2.cursors['key'] = 'second'
      peer1.cursors['key'] = 'third'
      peer1.process_message(peer2.make_message)
      peer2.process_message(peer1.make_message)
      expect(peer1.cursors.to_a.sort).to eq [['key', 'second']]
      expect(peer2.cursors.to_a.sort).to eq [['key', 'second']]
    end
  end
end
