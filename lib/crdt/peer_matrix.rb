module CRDT
  # Keeps track of what each peer knows about each other peer. This structure saves us from having
  # to send around full vector clocks all the time: instead, peers can just send diffs of their
  # vector clock when it is updated, and those diffs are applied to this matrix. It also enables
  # us to use a peer index (a small number) instead of a full 256-bit peer ID, which further
  # reduces message sizes.
  #
  # Each peer has its own mapping from peer IDs to peer indexes (to avoid having to coordinate
  # between peers to agree on a mapping). The only requirement is that for each peer, index 0 is
  # that peer itself. The other indexes are assigned sequentially in arbitrary order by each peer.
  # This matrix keeps track of each peer's assignment of peer indexes to peer IDs.
  class PeerMatrix

    PeerVClockEntry = Struct.new(:peer_id, :peer_index, :op_count)

    # matrix is an array of arrays (i.e. a 2D array).
    # matrix[peer1_index][peer2_index] is a PeerVClockEntry object.
    # Each such object records how many operations peer1 has seen from peer2.
    # peer1_index is according to this peer's local index assignment (see index_by_peer_id);
    # peer2_index is according to peer1's index assignment.
    attr_reader :matrix

    # A hash, where the key is a peer ID (as hex string) and the value is the index that this peer
    # has locally assigned to that peer ID. The indexes must be strictly sequential.
    attr_reader :index_by_peer_id

    # A hash, where the key is a peer ID (as hex string) and the value is a PeerVClockEntry
    # object. This is used to record any operations we see from other peers, so that we can
    # broadcast vector clock diffs to others.
    attr_reader :update_by_peer_id

    def initialize(own_peer_id)
      @matrix = [[PeerVClockEntry.new(own_peer_id, 0, 0)]]
      @index_by_peer_id = {own_peer_id => 0}
      @update_by_peer_id = {}
    end

    def own_peer_id
      @matrix[0][0].peer_id
    end

    # Translates a globally unique peer ID into a local peer index. If the peer ID is not already
    # known, it is added to the matrix and assigned a new index.
    def peer_id_to_index(peer_id)
      index = @index_by_peer_id[peer_id]
      return index if index

      if (@index_by_peer_id.size != @matrix.size) ||
         (@index_by_peer_id.size != @matrix[0].size) ||
          @matrix[0].any? {|entry| entry.peer_id == peer_id }
        raise 'Mismatch between vector clock and peer list'
      end

      index = @index_by_peer_id.size
      @index_by_peer_id[peer_id] = index
      @matrix[0][index] = PeerVClockEntry.new(peer_id, index, 0)
      @matrix[index] = [PeerVClockEntry.new(peer_id, 0, 0)]
      @update_by_peer_id[peer_id] = PeerVClockEntry.new(peer_id, index, 0)
      index
    end

    # For an incoming message from the given peer ID, check that the peer is set up in our local
    # state, and that the operation counter lines up with what we were expecting.
    def process_incoming_op_count(origin_peer_id, origin_op_count)
      origin_index = peer_id_to_index(origin_peer_id)

      # We normally expect the opCount for a peer to be monotonically increasing. However, there's
      # a possible scenario in which a peer sends some messages and then crashes before writing its
      # state to stable storage, so when it comes back up, it reverts back to a lower opCount.
      # We should detect when this happens, and replay the lost messages from another peer.
      entry = @matrix[0][origin_index]
      raise "peerID mismatch: #{entry.peer_id} != #{origin_peer_id}" if entry.peer_id != origin_peer_id
      raise "opCount for #{origin_peer_id} went backwards"  if entry.op_count > origin_op_count
      raise "opCount for #{origin_peer_id} jumped forwards" if entry.op_count < origin_op_count
    end

    # Processes a clock update from a remote peer and applies it to the local state.
    # This update indicates that +origin_peer_id+ has received +op_count+ operations from the peer
    # with ID +subject_peer_id+. Moreover, +origin_peer_id+ has assigned index
    # +subject_peer_index+ to that peer. +subject_peer_id+ may be left nil if +origin_peer_id+ has
    # already broadcast its mapping from +subject_peer_id+ to +subject_peer_index+ previously.
    def clock_update(origin_peer_id, subject_peer_id, subject_peer_index, op_count)
      vclock = @matrix[@index_by_peer_id[origin_peer_id]]
      entry = vclock[subject_peer_index]

      if entry.nil?
        raise 'Non-consecutive peer index assignment' if subject_peer_index != vclock.size
        raise 'New peer index assignment without ID' if subject_peer_id.nil?
        entry = PeerVClockEntry.new(subject_peer_id, subject_peer_index, op_count)
        vclock[subject_peer_index] = entry
      else
        entry.op_count = op_count
      end
    end

    # Increments the operation counter for the local peer, indicates that an operation has been
    # performed locally.
    def local_operation
      @matrix[0][0].op_count += 1
    end

    # Increments the operation counter for a particular peer, indicating that we have processed an
    # operation that originated on that peer. In other words, this moves the vector clock forward.
    def increment_op_count(origin_peer_id)
      origin_index = peer_id_to_index(origin_peer_id)

      local_vclock_entry = @matrix[0][origin_index]
      local_vclock_entry.op_count += 1

      remote_vclock_entry = @matrix[origin_index][0]
      remote_vclock_entry.op_count += 1

      @update_by_peer_id[origin_peer_id] ||= PeerVClockEntry.new(nil, origin_index, 0)
      @update_by_peer_id[origin_peer_id].op_count = local_vclock_entry.op_count
    end
  end
end
