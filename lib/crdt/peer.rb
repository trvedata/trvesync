require 'openssl'

module CRDT
  # A pair of logical timestamp (Lamport clock, which is just a number) and peer ID (256-bit hex
  # string that uniquely identifies a particular device). A peer increments its timestamp on every
  # operation, so this pair uniquely identifies a particular object, e.g. an element in a list.
  # It also provides a total ordering that is consistent with causality: if operation A happened
  # before operation B, then A's ItemID is lower than B's ItemID. The ordering of concurrent
  # operations is deterministic but arbitrary.
  class ItemID < Struct.new(:logical_ts, :peer_id)
    include Comparable

    def <=>(other)
      return +1 if self.logical_ts > other.logical_ts
      return -1 if self.logical_ts < other.logical_ts
      self.peer_id <=> other.peer_id
    end
  end

  class Peer
    include Encoding

    # 256-bit hex string that uniquely identifies this peer.
    attr_reader :peer_id

    # Keeps track of the key facts that we know about our peers.
    attr_reader :peer_matrix

    # CRDT data structure (TODO generalise this)
    attr_reader :ordered_list

    # Lamport clock
    attr_reader :logical_ts

    # Loads a peer's state from a file with the specified +filename+ path.
    def self.load(filename)
      Encoding.load(filename)
    end

    # Initializes a new peer instance with default state. If no peer ID is given, it is assigned a
    # new random peer ID (256-bit hex string).
    def initialize(peer_id=nil)
      @peer_id = peer_id || bin_to_hex(OpenSSL::Random.random_bytes(32))
      @peer_matrix = PeerMatrix.new(@peer_id)
      @ordered_list = OrderedList.new(self)
      @logical_ts = 0
      @operations = []
    end

    def anything_to_send?
      !@peer_matrix.update_by_peer_id.empty? || !@operations.empty?
    end

    def next_id
      @logical_ts += 1
      ItemID.new(@logical_ts, peer_id)
    end

    def send_operation(operation)
      @operations << operation
      @peer_matrix.local_operation
    end

    # Returns a list of operations that should be sent to remote sites.
    # Resets the list, so the same operations won't be returned again.
    def flush_operations
      return_ops = @operations
      @operations = []
      return_ops
    end

    def receive_operation(operation)
      if @logical_ts < operation.logical_ts
        @logical_ts = operation.logical_ts
      end
      ordered_list.apply_operation(operation)
    end

    def receive_operations(operations)
      operations.each {|op| receive_operation(op) }
    end
  end
end
