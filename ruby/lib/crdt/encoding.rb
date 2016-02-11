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
    CLIENT_TO_SERVER_SCHEMA = schemas['org.trvedata.trvedb.avro.ClientToServer']
    SERVER_TO_CLIENT_SCHEMA = schemas['org.trvedata.trvedb.avro.ServerToClient']

    # Loads a peer's state from a the specified IO object +file+.
    def self.load(file, options={})
      reader = Avro::DataFile::Reader.new(file, Avro::IO::DatumReader.new)
      peer_state = reader.first
      return if peer_state.nil?
      peer_id = peer_state['peers'][0]['peerID'].unpack('H*').first
      peer = Peer.new(peer_id, options)
      peer.from_avro_hash(peer_state)
      peer
    end

    # Writes the state of this peer out to the specified IO object +file+ as an Avro datafile.
    def save(file)
      writer = Avro::IO::DatumWriter.new(PEER_STATE_SCHEMA)
      io = Avro::DataFile::Writer.new(file, writer, PEER_STATE_SCHEMA)
      io << to_avro_hash
      io.flush
    end

    # Returns the state of this peer as a structure of nested hashes and lists, according to the
    # +PeerState+ schema.
    def to_avro_hash
      {
        'channelID'     => hex_to_bin(channel_id),
        'channelOffset' => channel_offset,
        'logicalTS'     => logical_ts,
        'peers'         => encode_peer_matrix,
        'data'          => encode_ordered_list
      }
    end

    # Loads the state of this peer from a structure of nested hashes and lists, according to the
    # +PeerState+ schema.
    def from_avro_hash(state)
      self.channel_id     = bin_to_hex(state['channelID'])
      self.channel_offset = state['channelOffset']
      self.logical_ts     = state['logicalTS']
      decode_peer_matrix(state['peers'])
      decode_ordered_list(state['data'])
    end

    # Parses an array of PeerEntry records, as decoded from Avro, and applies them to a PeerMatrix
    # object. Assumes the matrix is previously empty. The input has the following structure:
    #
    #     [
    #       {
    #         'peerID' => 'asdfasdf'
    #         'vclock' => [
    #           {
    #             'peerID' => 'asdfasdf',
    #             'msgCount' => 42
    #           },
    #           ...
    #         ]
    #       },
    #       ...
    #     ]
    def decode_peer_matrix(peer_list)
      peer_list.each_with_index do |peer, index|
        origin_peer_id = bin_to_hex(peer['peerID'])
        assigned_index = peer_matrix.peer_id_to_index(origin_peer_id)
        raise "Index mismatch: #{index} != #{assigned_index}" if index != assigned_index
      end

      peer_list.each do |peer|
        origin_peer_id = bin_to_hex(peer['peerID'])
        entries = []

        peer['vclock'].each_with_index do |entry, entry_index|
          entries << PeerMatrix::PeerVClockEntry.new(
            bin_to_hex(entry['peerID']), entry_index, entry['msgCount'])
        end
        peer_matrix.apply_clock_update(origin_peer_id, PeerMatrix::ClockUpdate.new(entries))
      end
    end

    # Transforms a PeerMatrix object into a structure corresponding to an array of PeerEntry
    # records, for encoding to Avro.
    def encode_peer_matrix
      peer_list = []
      peer_matrix.index_by_peer_id.each do |peer_id, peer_index|
        peer_list[peer_index] = {
          'peerID' => hex_to_bin(peer_id),
          'vclock' => peer_matrix.matrix[peer_index].map {|entry|
            {'peerID' => hex_to_bin(entry.peer_id), 'msgCount' => entry.msg_count}
          }
        }
      end
      peer_list
    end

    # Parses an Avro OrderedList record, and applies them to the current peer's data structure.
    # It has the following structure:
    #
    #     {'items' => [
    #       {
    #         # Uniquely identifies this list element
    #         'id' => {'logicalTS' => 123, 'peerIndex' => 3},
    #
    #         # Value of the element (nil if the item has been deleted)
    #         'value' => 'a'
    #
    #         # Tombstone timestamp if the item has been deleted (nil if it has not been deleted)
    #         'deleteTS' => {'logicalTS' => 321, 'peerIndex' => 0}
    #       },
    #       ...
    #     ]}
    def decode_ordered_list(hash)
      items = hash['items'].map do |item|
        insert_id = decode_item_id(peer_id, item['id'])
        delete_ts = decode_item_id(peer_id, item['deleteTS'])
        OrderedList::Item.new(insert_id, delete_ts, item['value'], nil, nil)
      end
      ordered_list.load_items(items)
    end

    # Transforms a CRDT::OrderedList object into a structure corresponding to the Avro OrderedList
    # record schema.
    def encode_ordered_list
      items = ordered_list.dump_items.map do |item|
        {
          'id'       => encode_item_id(item.insert_id),
          'value'    => item.value,
          'deleteTS' => encode_item_id(item.delete_ts)
        }
      end
      {'items' => items}
    end

    # Transforms a CRDT::Peer::Message object into an Avro-encoded byte string that should be
    # broadcast to all peers.
    def encode_message_payload(message)
      operations = message.operations.map do |operation|
        case operation
        when PeerMatrix::ClockUpdate then encode_clock_update(operation)
        when OrderedList::InsertOp   then encode_list_insert(operation)
        when OrderedList::DeleteOp   then encode_list_delete(operation)
        end
      end

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
      Avro::IO::DatumWriter.new(MESSAGE_SCHEMA).write({'operations' => operations}, encoder)
      encoder.writer.string
    end

    # Decodes an incoming message from another peer, given as an Avro-encoded byte string.
    # Returns an array of operation objects.
    def decode_message_payload(sender_id, serialized)
      serialized = StringIO.new(serialized) unless serialized.respond_to? :read
      decoder = Avro::IO::BinaryDecoder.new(serialized)
      reader = Avro::IO::DatumReader.new(MESSAGE_SCHEMA)
      message = reader.read(decoder)

      message['operations'].map do |operation|
        if operation['updates'] # ClockUpdate message
          decode_clock_update(sender_id, operation)
        elsif operation['newID'] # OrderedListInsert message
          decode_list_insert(sender_id, operation)
        elsif operation['deleteID'] # OrderedListDelete message
          decode_list_delete(sender_id, operation)
        else
          raise "Unexpected operation type: #{operation.inspect}"
        end
      end
    end

    # Decodes a ServerToClient command received from a WebSocket server. It has the following
    # structure:
    #
    #     {'message' => {
    #       # Identifies the channel on the server to which the message belongs
    #       'channelID'   => '...',
    #
    #       # PeerID of the sender of the message
    #       'senderID'    => 'asdfasdf',
    #
    #       # Per-peerID, per-channel message sequence number (must increment by 1 for every message)
    #       'senderSeqNo' => 12,
    #
    #       # Per-channel monotonically increasing (not necessarily incrementing) number
    #       'offset'      => 123,
    #
    #       # Avro-encoded message payload as a byte string (not interpreted by the server)
    #       'payload'     => '...'
    #     }}
    def receive_message(serialized)
      serialized = StringIO.new(serialized) unless serialized.respond_to? :read
      decoder = Avro::IO::BinaryDecoder.new(serialized)
      reader = Avro::IO::DatumReader.new(SERVER_TO_CLIENT_SCHEMA)
      message = reader.read(decoder)['message']

      sender_id = bin_to_hex(message['senderID'])
      if bin_to_hex(message['channelID']) != channel_id
        raise "Received message on unexpected channel: #{bin_to_hex(message['channelID'])}"
      end

      process_message(Peer::Message.new(sender_id, message['senderSeqNo'], message['offset'],
        decode_message_payload(sender_id, message['payload'])))
    end

    # Takes all accumulated changes since the last call to #encode_message, and constructs
    # a SendMessage command to send to a WebSocket server. It has the following structure:
    #
    #     {
    #       # Identifies the channel on the server to which the message should be broadcast
    #       'channelID' => '...',
    #
    #       # Per-peerID, per-channel message sequence number (must increment by 1 for every message)
    #       'senderSeqNo' => 123,
    #
    #       # Avro-encoded message payload as a byte string (not interpreted by the server)
    #       'payload' => '...'
    #     }
    def encode_message
      message = make_message

      message_hash = {
        'channelID'   => hex_to_bin(channel_id),
        'senderSeqNo' => message.msg_count,
        'payload'     => encode_message_payload(message)
      }

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
      writer = Avro::IO::DatumWriter.new(CLIENT_TO_SERVER_SCHEMA)
      writer.write({'message' => message_hash}, encoder)
      encoder.writer.string
    end

    # Constructs a SubscribeToChannel command to send to a WebSocket server. It has the following
    # structure:
    #
    #     {
    #       # Identifies the channel to which the client wants to subscribe
    #       'channelID' => '...',
    #
    #       # Offset of the last message received from this channel (-1 if nothing received yet)
    #       'startOffset' => 123
    #     }
    def encode_subscribe_request
      message_hash = {
        'channelID'   => hex_to_bin(channel_id),
        'startOffset' => channel_offset
      }

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
      writer = Avro::IO::DatumWriter.new(CLIENT_TO_SERVER_SCHEMA)
      writer.write({'message' => message_hash}, encoder)
      encoder.writer.string
    end

    # Decodes a ClockUpdate message from a remote peer. It has the following structure:
    #
    #     {'updates' => [
    #       # Assigns peerIndex = 3 to this peerID
    #       {'peerID' => 'asdfasdf', 'peerIndex' => 3, 'msgCount' => 5},
    #
    #       # peerID can be omitted if peerIndex was previously assigned
    #       {'peerID' => nil, 'peerIndex' => 2, 'msgCount' => 42},
    #       ...
    #     ]}
    def decode_clock_update(origin_peer_id, operation)
      entries = operation['updates'].map do |entry|
        subject_peer_id = entry['peerID'] ? bin_to_hex(entry['peerID']) : nil

        # Need to register the peer index mapping right now, even though we don't apply the clock
        # update until later, because the peer index mapping may be needed to decode subsequent
        # operations.
        peer_matrix.peer_index_mapping(origin_peer_id, subject_peer_id, entry['peerIndex'])

        PeerMatrix::PeerVClockEntry.new(subject_peer_id, entry['peerIndex'], entry['msgCount'])
      end

      PeerMatrix::ClockUpdate.new(entries)
    end

    # Encodes a CRDT::PeerMatrix::ClockUpdate operation for sending over the network.
    def encode_clock_update(clock_update)
      entries = clock_update.entries.map do |entry|
        {
          'peerID'    => entry.peer_id ? hex_to_bin(entry.peer_id) : nil,
          'peerIndex' => entry.peer_index,
          'msgCount'  => entry.msg_count
        }
      end
      {'updates' => entries}
    end

    # Parses an OrderedListInsert message. It has the following structure:
    #
    #     {
    #       # Identifies the list element to the left of the element being inserted (nil if head of list)
    #       'referenceID' => {'logicalTS' => 123, 'peerIndex' => 3},
    #
    #       # New identifier of the element being inserted
    #       'newID' => {'logicalTS' => 321, 'peerIndex' => 0},
    #
    #       # Value of the element being inserted
    #       'value' => 'a'
    #     }
    def decode_list_insert(origin_peer_id, operation)
      OrderedList::InsertOp.new(decode_item_id(origin_peer_id, operation['referenceID']),
                                decode_item_id(origin_peer_id, operation['newID']),
                                operation['value'])
    end

    # Encodes a CRDT::OrderedList::InsertOp operation for sending over the network.
    def encode_list_insert(operation)
      {
        'referenceID' => encode_item_id(operation.reference_id),
        'newID'       => encode_item_id(operation.new_id),
        'value'       => operation.value
      }
    end

    # Parses an OrderedListDelete message. It has the following structure:
    #
    #     {
    #       # Identifies the list element being deleted
    #       'deleteID' => {'logicalTS' => 123, 'peerIndex' => 3},
    #
    #       # Tombstone timestamp for the deletion
    #       'deleteTS' => {'logicalTS' => 321, 'peerIndex' => 0}
    #     }
    def decode_list_delete(origin_peer_id, operation)
      OrderedList::DeleteOp.new(decode_item_id(origin_peer_id, operation['deleteID']),
                                decode_item_id(origin_peer_id, operation['deleteTS']))
    end

    # Encodes a CRDT::OrderedList::DeleteOp operation for sending over the network.
    def encode_list_delete(operation)
      {
        'deleteID' => encode_item_id(operation.delete_id),
        'deleteTS' => encode_item_id(operation.delete_ts)
      }
    end

    # Translates an on-the-wire encoding of an ItemID (using a peer index) into an in-memory ItemID
    # object (using a peer ID).
    def decode_item_id(origin_peer_id, hash)
      return nil if hash.nil?
      peer_id = peer_matrix.remote_index_to_peer_id(origin_peer_id, hash['peerIndex'])
      ItemID.new(hash['logicalTS'], peer_id)
    end

    # Translates an in-memory ItemID object (using a peer ID) into an on-the-wire encoding of an
    # ItemID (using a peer index).
    def encode_item_id(item_id)
      return nil if item_id.nil?
      {
        'logicalTS' => item_id.logical_ts,
        'peerIndex' => peer_matrix.peer_id_to_index(item_id.peer_id)
      }
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
