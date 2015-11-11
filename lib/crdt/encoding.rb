require 'yaml'
require 'avro'
require 'stringio'

module CRDT
  # Mixin for CRDT::Peer. Contains all the logic related to saving and loading a peer's state
  # to/from disk, and related to sending and receiving messages over a network.
  module Encoding
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

    # Loads a peer's state from a file with the specified +filename+ path.
    def self.load(filename)
      peer = nil
      Avro::DataFile.open(filename) do |io|
        peer_state = io.first
        peer_id = peer_state['peers'][0]['peerID'].unpack('H*').first
        peer = Peer.new(peer_id)
        peer.from_avro_hash(peer_state)
      end
      peer
    end

    # Writes the state of this peer out to an Avro datafile. The +file+ parameter must be an open,
    # writable file handle. It will be closed after writing.
    def save(file)
      writer = Avro::IO::DatumWriter.new(PEER_STATE_SCHEMA)
      io = Avro::DataFile::Writer.new(file, writer, PEER_STATE_SCHEMA)
      io << to_avro_hash
    ensure
      io.close
    end

    # Returns the state of this peer as a structure of nested hashes and lists, according to the
    # +PeerState+ schema.
    def to_avro_hash
      {
        'logicalTS' => 0,
        'peers' => matrix_to_peer_list,
        'data' => {'items' => []}
      }
    end

    # Loads the state of this peer from a structure of nested hashes and lists, according to the
    # +PeerState+ schema.
    def from_avro_hash(state)
      peer_list_to_matrix(state['peers'])
    end

    # Transforms a PeerMatrix object into a structure corresponding to an array of PeerEntry
    # records, for encoding to Avro.
    def matrix_to_peer_list
      peer_list = []
      peer_matrix.index_by_peer_id.each do |peer_id, peer_index|
        peer_list[peer_index] = {
          'peerID' => hex_to_bin(peer_id),
          'vclock' => peer_matrix.matrix[peer_index].map {|entry|
            {'peerID' => hex_to_bin(entry.peer_id), 'opCount' => entry.op_count}
          }
        }
      end
      peer_list
    end

    # Parses an array of PeerEntry records, as decoded from Avro, and applies them to a PeerMatrix
    # object. Assumes the matrix is previously empty.
    def peer_list_to_matrix(peer_list)
      peer_list.each_with_index do |peer, index|
        origin_peer_id = bin_to_hex(peer['peerID'])
        assigned_index = peer_matrix.peer_id_to_index(origin_peer_id)
        raise "Index mismatch: #{index} != #{assigned_index}" if index != assigned_index

        peer['vclock'].each_with_index do |entry, entry_index|
          peer_matrix.clock_update(origin_peer_id, bin_to_hex(entry['peerID']), entry_index, entry['opCount'])
        end
      end
    end

    # Takes all accumulated changes since the last call to #encode_message, and returns them as an
    # Avro-encoded byte string that should be broadcast to all peers.
    def encode_message
      message = {
        'origin' => hex_to_bin(peer_id),
        'opCount' => peer_matrix.matrix[0][0].op_count - @operations.size,
        'operations' => []
      }

      unless peer_matrix.update_by_peer_id.empty?
        message['operations'] << {'updates' => encode_clock_updates}
      end

      message['operations'].concat(@operations)
      @operations = []

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
      Avro::IO::DatumWriter.new(MESSAGE_SCHEMA).write(message, encoder)
      encoder.writer.string
    end

    def encode_clock_updates
      updates = peer_matrix.update_by_peer_id.values.map do |update|
        {
          'peerID' => update.peer_id ? hex_to_bin(update.peer_id) : nil,
          'peerIndex' => update.peer_index,
          'opCount' => update.op_count
        }
      end
      peer_matrix.update_by_peer_id.clear
      updates
    end

    # Receives an incoming message from another peer, given as an Avro-encoded byte string.
    def receive_message(serialized)
      serialized = StringIO.new(serialized) unless serialized.respond_to? :read
      decoder = Avro::IO::BinaryDecoder.new(serialized)
      reader = Avro::IO::DatumReader.new(MESSAGE_SCHEMA)
      message = reader.read(decoder)

      origin_peer_id = bin_to_hex(message['origin'])
      peer_matrix.process_incoming_op_count(origin_peer_id, message['opCount'])

      message['operations'].each do |operation|
        if operation['opCount'] # ClockUpdate message
          process_clock_update(origin_peer_id, operation)
        elsif operation['newID'] # OrderedListInsert message
          process_list_insert(origin_peer_id, operation)
        elsif operation['deleteID'] # OrderedListDelete message
          process_list_delete(origin_peer_id, operation)
        else
          raise "Unexpected operation type: #{operation.inspect}"
        end
      end
    end

    # Processes a ClockUpdate message from a remote peer and applies it to the local state.
    def process_clock_update(origin_peer_id, operation)
      operation['updates'].each do |update|
        subject_peer_id = bin_to_hex(update['peerID']) if update['peerID']
        peer_matrix.clock_update(origin_peer_id, subject_peer_id, update['peerIndex'], update['opCount'])
      end
    end

    # Translates a binary string into a hex string
    def bin_to_hex(binary)
      binary.unpack('H*').first
    end

    # Translates a hex string into a binary string
    def hex_to_bin(hex)
      [hex].pack('H*')
    end
  end
end
