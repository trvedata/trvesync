require 'crdt'

RSpec.describe CRDT::PeerMatrix do
  it 'should assign sequential message numbers' do
    peer = CRDT::Peer.new
    peer.ordered_list.insert(0, :a)
    expect(peer.make_message.msg_count).to eq 1
    peer.ordered_list.insert(1, :b)
    expect(peer.make_message.msg_count).to eq 2
    peer.ordered_list.insert(2, :c).delete(0)
    expect(peer.make_message.msg_count).to eq 3
  end

  it 'should assign peer indexes in the order they are seen' do
    local = CRDT::Peer.new
    other_peer_ids = [:a, :b, :c].map do |letter|
      remote = CRDT::Peer.new
      remote.ordered_list.insert(0, letter)
      local.process_message(remote.make_message)
      remote.peer_id
    end

    expect(local.peer_matrix.peer_id_to_index(local.peer_id)).to eq 0
    expect(local.peer_matrix.peer_id_to_index(other_peer_ids[0])).to eq 1
    expect(local.peer_matrix.peer_id_to_index(other_peer_ids[1])).to eq 2
    expect(local.peer_matrix.peer_id_to_index(other_peer_ids[2])).to eq 3
  end

  it 'should generate clock update operations when messages are received' do
    local, remote1, remote2 = CRDT::Peer.new, CRDT::Peer.new, CRDT::Peer.new
    remote1.ordered_list.insert(0, :a).insert(1, :b)
    remote2.ordered_list.insert(0, :z)
    local.process_message(remote1.make_message)
    local.process_message(remote2.make_message)
    remote1.ordered_list.insert(2, :c)
    local.process_message(remote1.make_message)

    clock_update = local.make_message.operations.first
    expect(clock_update).to be_a(CRDT::PeerMatrix::ClockUpdate)
    expect(clock_update.entries).to eq [
      CRDT::PeerMatrix::PeerVClockEntry.new(remote1.peer_id, 1, 2),
      CRDT::PeerMatrix::PeerVClockEntry.new(remote2.peer_id, 2, 1)
    ]
  end

  it 'should include the peer ID only on the first clock update' do
    local = CRDT::Peer.new
    remote1 = CRDT::Peer.new
    remote1.ordered_list.insert(0, :a)
    local.process_message(remote1.make_message)
    expect(local.make_message.operations.first.entries).to eq [
      CRDT::PeerMatrix::PeerVClockEntry.new(remote1.peer_id, 1, 1)
    ]

    remote2 = CRDT::Peer.new
    remote2.ordered_list.insert(0, :a)
    local.process_message(remote2.make_message)
    expect(local.make_message.operations.first.entries).to eq [
      CRDT::PeerMatrix::PeerVClockEntry.new(remote2.peer_id, 2, 1)
    ]

    remote1.ordered_list.insert(0, :a)
    local.process_message(remote1.make_message)
    expect(local.make_message.operations.first.entries).to eq [
      CRDT::PeerMatrix::PeerVClockEntry.new(nil, 1, 2)
    ]
  end

  it 'should decode remote peer indexes' do
    peer1, peer2, peer3 = CRDT::Peer.new, CRDT::Peer.new, CRDT::Peer.new
    peer1.ordered_list.insert(0, :a)
    msg1 = peer1.make_message

    peer2.process_message(msg1)
    peer2.ordered_list.insert(1, :b)
    msg2 = peer2.make_message

    peer3.process_message(msg2)
    peer3.process_message(msg1)
    peer3.ordered_list.insert(2, :c)
    msg3 = peer3.make_message

    peer1.process_message(msg2)
    peer1.process_message(msg3)
    peer2.process_message(msg3)
    peer1.process_message(peer2.make_message)

    expect(peer1.peer_matrix.remote_index_to_peer_id(peer2.peer_id, 0)).to eq peer2.peer_id
    expect(peer1.peer_matrix.remote_index_to_peer_id(peer2.peer_id, 1)).to eq peer1.peer_id
    expect(peer1.peer_matrix.remote_index_to_peer_id(peer2.peer_id, 2)).to eq peer3.peer_id

    expect(peer1.peer_matrix.remote_index_to_peer_id(peer3.peer_id, 0)).to eq peer3.peer_id
    expect(peer1.peer_matrix.remote_index_to_peer_id(peer3.peer_id, 1)).to eq peer2.peer_id
    expect(peer1.peer_matrix.remote_index_to_peer_id(peer3.peer_id, 2)).to eq peer1.peer_id

    expect(peer2.peer_matrix.remote_index_to_peer_id(peer1.peer_id, 0)).to eq peer1.peer_id
    expect(peer2.peer_matrix.remote_index_to_peer_id(peer3.peer_id, 0)).to eq peer3.peer_id
    expect(peer2.peer_matrix.remote_index_to_peer_id(peer3.peer_id, 1)).to eq peer2.peer_id
    expect(peer2.peer_matrix.remote_index_to_peer_id(peer3.peer_id, 2)).to eq peer1.peer_id

    expect(peer3.peer_matrix.remote_index_to_peer_id(peer1.peer_id, 0)).to eq peer1.peer_id
    expect(peer3.peer_matrix.remote_index_to_peer_id(peer2.peer_id, 0)).to eq peer2.peer_id
    expect(peer3.peer_matrix.remote_index_to_peer_id(peer2.peer_id, 1)).to eq peer1.peer_id
  end

  it 'should track causal dependencies across peers' do
    peer1, peer2, peer3 = CRDT::Peer.new, CRDT::Peer.new, CRDT::Peer.new
    peer1.ordered_list.insert(0, :a)
    msg1 = peer1.make_message

    peer2.process_message(msg1)
    peer2.ordered_list.insert(1, :b)
    msg2 = peer2.make_message

    peer3.process_message(msg2)
    expect(peer3.peer_matrix.causally_ready?(peer2.peer_id)).to be false
    peer3.process_message(msg1)
    expect(peer3.peer_matrix.causally_ready?(peer2.peer_id)).to be true
    expect(peer3.peer_matrix.causally_ready?(peer1.peer_id)).to be true
  end
end
