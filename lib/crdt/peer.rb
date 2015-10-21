require 'yaml'
require 'avro'
require 'openssl'
require 'stringio'

module CRDT
  class Peer
    SCHEMAS_FILE = File.expand_path('schemas.yaml', File.dirname(__FILE__))

    # Loads the Avro schemas from the Yaml file, and returns a hash from schema name to schema.
    def self.schemas
      return @schemas if @schemas
      @schemas = {}
      YAML.load_file(SCHEMAS_FILE).each do |type|
        Avro::Schema.real_parse(type, @schemas)
      end
      @schemas
    end

    MESSAGE_SCHEMA = schemas['Message']
    PEER_STATE_SCHEMA = schemas['PeerState']

    # Creates a new peer with a random ID.
    def self.create
      peer_id = OpenSSL::Random.random_bytes(16)
      new(
        'logicalTS' => 0,
        'peers' => [{
          'peerID' => peer_id,
          'vclock' => [{'peerID' => peer_id, 'opCount' => 0}]
        }],
        'data' => {'items' => []}
      )
    end

    # Loads a peer's state from a file with the specified +filename+ path.
    def self.load(filename)
      Avro::DataFile.open(filename) do |io|
        return new(io.first)
      end
    end

    # Initializes a new peer instance with a given state. Use Peer.create to create a peer with
    # default state.
    def initialize(state)
      @state = state
      @clock_updates = []
      @operations = []
    end

    # The ID of this peer, as a hex string.
    def peer_id
      to_hex(@state['peers'][0]['peerID'])
    end

    # Writes the state of this peer out to an Avro datafile. The +file+ parameter must be an open,
    # writable file handle. It will be closed after writing.
    def save(file)
      writer = Avro::IO::DatumWriter.new(PEER_STATE_SCHEMA)
      io = Avro::DataFile::Writer.new(file, writer, PEER_STATE_SCHEMA)
      io << @state
    ensure
      io.close
    end

    def anything_to_send?
      @clock_updates.any? || @operations.any?
    end

    # Takes all accumulated changes since the last call to #encode_message, and returns them as an
    # Avro-encoded byte string that should be broadcast to all peers.
    def encode_message
      local_peer = @state['peers'][0]
      message = {
        'origin' => local_peer['peerID'],
        'opCount' => local_peer['vclock'][0]['opCount'] - @operations.size,
        'operations' => []
      }

      if @clock_updates.any?
        message['operations'] << {'updates' => @clock_updates.compact}
      end

      message['operations'].concat(@operations)
      @clock_updates = []
      @operations = []

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
      Avro::IO::DatumWriter.new(MESSAGE_SCHEMA).write(message, encoder)
      encoder.writer.string
    end

    # Receives an incoming message from another peer, given as an Avro-encoded byte string.
    def receive_message(serialized)
      serialized = StringIO.new(serialized) unless serialized.respond_to? :read
      decoder = Avro::IO::BinaryDecoder.new(serialized)
      reader = Avro::IO::DatumReader.new(MESSAGE_SCHEMA)
      message = reader.read(decoder)
      process_incoming_op_count(message['origin'], message['opCount'])

      message['operations'].each do |operation|
        if operation['opCount'] # ClockUpdate message
          process_clock_update(message['origin'], operation)
        elsif operation['newID'] # OrderedListInsert message
          process_list_insert(message['origin'], operation)
        elsif operation['deleteID'] # OrderedListDelete message
          process_list_delete(message['origin'], operation)
        else
          raise "Unexpected operation type: #{operation.inspect}"
        end
      end
    end

    # TODO placeholder
    def local_operation
      @operations << {
        'referenceID' => nil,
        'newID' => {'logicalTS' => 0, 'peerIndex' => 0},
        'value' => 'a'
      }
      @state['logicalTS'] += 1
      @state['peers'][0]['vclock'][0]['opCount'] += 1
    end

    private

    # Translates a binary string into a hex string
    def to_hex(binary)
      binary.unpack('H*').first
    end

    # Translates a globally unique peer ID into a local peer index. If the peer ID is not already
    # known, it is added to the local state and assigned a new index.
    def peer_id_to_index(peer_id)
      index = @state['peers'].find_index {|peer| peer['peerID'] == peer_id }
      return index if index

      if @state['peers'].size != @state['peers'][0]['vclock'].size ||
          @state['peers'][0]['vclock'].any? {|entry| entry['peerID'] == peer_id }
        raise 'Mismatch between vector clock and peer list'
      end

      index = @state['peers'].size
      @state['peers'] << {
        'peerID' => peer_id,
        'vclock' => [{'peerID' => peer_id, 'opCount' => 0}]
      }
      @state['peers'][0]['vclock'] << {'peerID' => peer_id, 'opCount' => 0}
      @clock_updates[index] = {'peerID' => peer_id, 'peerIndex' => index, 'opCount' => 0}
      index
    end

    # Looks up the vector clock we have for a peer by its globally unique ID. Returns an
    # array of the form: [{'peerID' => x, 'opCount' => x}, ...]
    def peer_vclock_by_id(peer_id)
      peer = @state['peers'].detect {|peer| peer['peerID'] == peer_id }
      raise "Unknown peer ID #{to_hex(peer_id)}" if peer.nil?
      peer['vclock']
    end

    # Takes a peer index that originated on another peer, and translates it into the globally
    # unique peer ID.
    def remote_peer_index_to_peer_id(origin_peer_id, remote_peer_index)
      vclock = peer_vclock_by_id(origin_peer_id)
      entry = vclock[remote_peer_index]
      raise "Unknown peer index #{remote_peer_index} from #{to_hex(origin_peer_id)}" if entry.nil?
      entry['peerID']
    end

    # For an incoming message from the given peer ID, check that the peer is set up in our local
    # state, and that the operation counter lines up with what we were expecting.
    def process_incoming_op_count(origin_peer_id, origin_op_count)
      origin_index = peer_id_to_index(origin_peer_id)

      # We normally expect the opCount for a peer to be monotonically increasing. However, there's
      # a possible scenario in which a peer sends some messages and then crashes before writing its
      # state to stable storage, so when it comes back up, it reverts back to a lower opCount.
      # We should detect when this happens, and replay the lost messages from another peer.
      local_vclock_entry = @state['peers'][0]['vclock'][origin_index]
      if local_vclock_entry['peerID'] != origin_peer_id
        raise "peerID mismatch: #{to_hex(local_vclock_entry['peerID'])} != #{to_hex(origin_peer_id)}"
      end
      if local_vclock_entry['opCount'] > origin_op_count
        raise "opCount for peer #{to_hex(origin_peer_id)} went backwards"
      end
      if local_vclock_entry['opCount'] < origin_op_count
        raise "opCount for peer #{to_hex(origin_peer_id)} jumped forwards"
      end
    end

    # Processes a ClockUpdate message from a remote peer and applies it to the local state.
    def process_clock_update(origin_peer_id, operation)
      origin_vclock = peer_vclock_by_id(origin_peer_id)

      operation['updates'].each do |update|
        entry = origin_vclock[update['peerIndex']]
        if entry.nil?
          raise 'Non-consecutive peer index assignment' if update['peerIndex'] != origin_vclock.size
          raise 'New peer index assignment without ID' if update['peerID'].nil?
          entry = {'peerID' => update['peerID']}
          origin_vclock << entry
        end
        entry['opCount'] = update['opCount']
      end
    end

    # Increments the operation counter for a particular peer, indicating that we have processed an
    # operation that originated on that peer. In other words, this moves the vector clock forward.
    def increment_op_count(origin_peer_id)
      origin_index = peer_id_to_index(origin_peer_id)

      local_vclock_entry = @state['peers'][0]['vclock'][origin_index]
      local_vclock_entry['opCount'] += 1

      remote_vclock_entry = @state['peers'][origin_index]['vclock'][0]
      remote_vclock_entry['opCount'] += 1

      @clock_updates[origin_index] ||= {'peerID' => nil, 'peerIndex' => origin_index}
      @clock_updates[origin_index]['opCount'] = local_vclock_entry['opCount']
    end

    def process_list_insert(origin_peer_id, operation)
      # TODO
      increment_op_count(origin_peer_id)
    end

    def process_list_delete(origin_peer_id, operation)
      # TODO
      increment_op_count(origin_peer_id)
    end
  end
end
