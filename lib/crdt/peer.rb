require 'openssl'

module CRDT
  class Peer
    include Encoding

    # 256-bit hex string that uniquely identifies this peer.
    attr_reader :peer_id

    # Keeps track of the key facts that we know about our peers.
    attr_reader :peer_matrix

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
      @logical_ts = 0
      @operations = []
    end

    def anything_to_send?
      !@peer_matrix.update_by_peer_id.empty? || !@operations.empty?
    end

    # TODO placeholder
    def local_operation
      @operations << {
        'referenceID' => nil,
        'newID' => {'logicalTS' => 0, 'peerIndex' => 0},
        'value' => 'a'
      }
      @logical_ts += 1
      @peer_matrix.local_operation
    end

    private

    def process_list_insert(origin_peer_id, operation)
      # TODO
      peer_matrix.increment_op_count(origin_peer_id)
    end

    def process_list_delete(origin_peer_id, operation)
      # TODO
      peer_matrix.increment_op_count(origin_peer_id)
    end
  end
end
