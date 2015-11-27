require 'crdt'
require 'tempfile'

RSpec.describe CRDT::Peer do
  before :each do
    @tempfiles = []
  end

  after :each do
    @tempfiles.each(&:close).each(&:unlink)
  end

  def new_tempfile
    Tempfile.new('crdt_peer', Dir.tmpdir, 'wb').tap do |file|
      @tempfiles << file
    end
  end

  def decode_msg(serialized)
    decoder = Avro::IO::BinaryDecoder.new(StringIO.new(serialized))
    reader = Avro::IO::DatumReader.new(CRDT::Peer::MESSAGE_SCHEMA)
    reader.read(decoder)
  end

  it 'should save and reload its state' do
    peer = CRDT::Peer.new
    peer.save(new_tempfile)
    reloaded = CRDT::Peer.load(@tempfiles.first.path)
    expect(reloaded.peer_id).to eq peer.peer_id
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
