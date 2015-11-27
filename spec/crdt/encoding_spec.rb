require 'crdt'

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

    it 'should save and reload the peer matrix'
    it 'should save and reload message buffers'
  end

  it 'should send a message from one peer to another' do
    peer1 = CRDT::Peer.new
    peer2 = CRDT::Peer.new
    peer1.ordered_list.insert(0, 'a')
    peer2.receive_message(peer1.encode_message.tap {|m| @msg1 = decode_msg(m) })
    @msg2 = decode_msg(peer2.encode_message)

    expect(@msg1['operations']).to eq([{
      'referenceID' => nil,
      'newID' => {'logicalTS' => 1, 'peerIndex' => 0},
      'value' => 'a'
    }])
    expect(@msg2['operations']).to eq([{'updates' => [{
      'peerID'    => @msg1['origin'],
      'peerIndex' => 1,
      'msgCount'  => 1
    }]}])
  end
end
