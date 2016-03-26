require 'crdt'

RSpec.describe CRDT::PeerMatrix do

  # make_peers(n) creates n peers on the same channel
  def make_peers(num_peers)
    peer0 = CRDT::Peer.new
    [peer0] + (1...num_peers).map do
      CRDT::Peer.new(nil, channel_id: peer0.channel_id)
    end
  end

  it 'should assign sequential message numbers' do
    peer = CRDT::Peer.new
    peer.ordered_list.insert(0, :a)
    expect(peer.make_message.sender_seq_no).to eq 1
    peer.ordered_list.insert(1, :b)
    expect(peer.make_message.sender_seq_no).to eq 2
    peer.ordered_list.insert(2, :c).delete(0)
    expect(peer.make_message.sender_seq_no).to eq 3
  end

  it 'should assign peer indexes in the order they are seen' do
    peers = make_peers(4)
    init_msg = peers[0].make_message

    (1..3).each do |num|
      peers[num].process_message(init_msg)
      peers[num].ordered_list.insert(0, num.to_s)
      peers[0].process_message(peers[num].make_message)
    end

    expect(peers[0].peer_matrix.peer_id_to_index(peers[0].peer_id)).to eq 0
    expect(peers[0].peer_matrix.peer_id_to_index(peers[1].peer_id)).to eq 1
    expect(peers[0].peer_matrix.peer_id_to_index(peers[2].peer_id)).to eq 2
    expect(peers[0].peer_matrix.peer_id_to_index(peers[3].peer_id)).to eq 3
  end

  it 'should generate clock update operations when messages are received' do
    local, remote1, remote2 = make_peers(3)
    init_msg = local.make_message
    remote1.process_message(init_msg)
    remote2.process_message(init_msg)

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
    local, remote1, remote2 = make_peers(3)
    init_msg = local.make_message
    remote1.process_message(init_msg)
    remote2.process_message(init_msg)

    remote1.ordered_list.insert(0, :a)
    local.process_message(remote1.make_message)
    expect(local.make_message.operations.grep(CRDT::PeerMatrix::ClockUpdate).first.entries).to eq [
      CRDT::PeerMatrix::PeerVClockEntry.new(remote1.peer_id, 1, 1)
    ]

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
    peer1, peer2, peer3 = make_peers(3)
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
    peer1, peer2, peer3 = make_peers(3)
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

  it 'should stop sending messages eventually after no more changes occur' do
    peer1, peer2 = make_peers(2)
    peer1.ordered_list.insert(0, :a)

    for i in 0..5  # 5 rounds should more than sufficient for peers to get into a stable state
      if not (peer1.anything_to_send? || peer2.anything_to_send?)
        break
      end

      if peer1.anything_to_send?
        peer2.process_message(peer1.make_message)
      end

      if peer2.anything_to_send?
        peer1.process_message(peer2.make_message)
      end
    end

    expect(peer1.anything_to_send? || peer2.anything_to_send?).to be false
  end
end
