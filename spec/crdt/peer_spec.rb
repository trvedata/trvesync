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

  it 'should save and reload its state' do
    peer = CRDT::Peer.create
    peer.save(new_tempfile)
    reloaded = CRDT::Peer.load(@tempfiles.first.path)
    expect(reloaded.peer_id).to eq peer.peer_id
  end
end
