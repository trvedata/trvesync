require 'yaml'
require 'avro'
require 'json'
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
    APP_OPERATION_SCHEMA = schemas['AppOperation']
    CLIENT_TO_SERVER_SCHEMA = schemas['org.trvedata.trvedb.avro.ClientToServer']
    SERVER_TO_CLIENT_SCHEMA = schemas['org.trvedata.trvedb.avro.ServerToClient']
    ITEM_ID_SCHEMA = Avro::Schema.real_parse(['null', 'ItemID'], schemas)

    APPLICATION_SCHEMA = JSON.generate({
      name: 'TextDocument',
      type: 'record',
      fields: [
        # NB. Stock Avro always uses strings as keys, this is an extension
        {name: 'cursors',    type: {type: 'map',   keys:  'PeerID', values: ['null', 'ItemID']}},
        {name: 'characters', type: {type: 'array', items: 'string'}}
      ]
    })

    # Loads a peer's state from a the specified IO object +file+.
    def self.load(file, options={})
      reader = Avro::DataFile::Reader.new(file, Avro::IO::DatumReader.new)
      peer_state = reader.first
      return if peer_state.nil?
      peer_id = peer_state['peers'][0]['peerID'].unpack('H*').first
      channel_id = peer_state['channelID'].unpack('H*').first
      peer = Peer.new(peer_id, options.merge(channel_id: channel_id))
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
      raise 'Cannot save peer without default_schema_id' if default_schema_id.nil?
      {
        'channelID'       => hex_to_bin(channel_id),
        'channelOffset'   => channel_offset, # obsolete (determined from message_log)
        'secretKey'       => secret_key && hex_to_bin(secret_key),
        'defaultSchemaID' => encode_item_id(default_schema_id),
        'cursorsItemID'   => encode_item_id(cursors_item_id),
        'charactersItemID'=> encode_item_id(characters_item_id),
        'peers'           => encode_peer_matrix,
        'messageLog'      => encode_message_log,
        'data'            => {
          'cursors'       => encode_cursors,
          'characters'    => encode_ordered_list
        }
      }
    end

    # Loads the state of this peer from a structure of nested hashes and lists, according to the
    # +PeerState+ schema.
    def from_avro_hash(state)
      decode_peer_matrix(state['peers'])
      decode_message_log(state['messageLog'])
      decode_cursors(state['data']['cursors'])
      decode_ordered_list(state['data']['characters'])
      self.channel_id     = bin_to_hex(state['channelID'])
      self.channel_offset = message_log.map(&:offset).compact.max || -1
      self.secret_key     = state['secretKey'] && bin_to_hex(state['secretKey'])
      self.default_schema_id  = decode_item_id(peer_id, state['defaultSchemaID'])
      self.cursors_item_id    = decode_item_id(peer_id, state['cursorsItemID'])
      self.characters_item_id = decode_item_id(peer_id, state['charactersItemID'])
      reload!
    end

    # Parses an array of PeerEntry records, as decoded from Avro, and applies them to a PeerMatrix
    # object. Assumes the matrix is previously empty. The input has the following structure:
    #
    #     [
    #       {
    #         'peerID' => 'asdfasdf'
    #         'nextTS' => 123,
    #         'vclock' => [
    #           {
    #             'peerIndex' => 3,
    #             'lastSeqNo' => 42
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
          peer_id = peer_matrix.peer_index_to_id(entry['peerIndex'])
          entries << PeerMatrix::PeerVClockEntry.new(peer_id, entry_index, entry['lastSeqNo'])
        end

        clock_update = PeerMatrix::ClockUpdate.new(peer['nextTS'], entries)
        peer_matrix.apply_clock_update(origin_peer_id, clock_update)
      end
    end

    # Transforms a PeerMatrix object into a structure corresponding to an array of PeerEntry
    # records, for encoding to Avro.
    def encode_peer_matrix
      peer_list = []
      peer_matrix.index_by_peer_id.each do |peer_id, peer_index|
        peer_list[peer_index] = {
          'peerID' => hex_to_bin(peer_id),
          'nextTS' => peer_matrix.next_ts_by_peer_id[peer_id] || 0,
          'vclock' => peer_matrix.matrix[peer_index].map {|entry|
            {
              'peerIndex' => peer_matrix.peer_id_to_index(entry.peer_id),
              'lastSeqNo' => entry.last_seq_no
            }
          }
        }
      end
      peer_list
    end

    # Parses an array of MessageLogEntry records, as decoded from Avro, and applies them to the
    # current peer. The input has the following structure:
    #
    #     [{
    #       # Local peerIndex of the peer that sent the message
    #       'senderPeerIndex' => 3,
    #
    #       # Per-sender sequence number of the message
    #       'senderSeqNo'     => 123,
    #
    #       # Server-assigned offset of the message (-1 if message is not yet confirmed by server)
    #       'offset'          => 1234,
    #
    #       # Binary string of encoded, encrypted message contents (output of encode_message_payload)
    #       'payload'         => '...'
    #      }, ...
    #     ]
    def decode_message_log(messages)
      messages.each do |hash|
        sender_id = peer_matrix.peer_index_to_id(hash['senderPeerIndex'])
        offset = hash['offset'] unless hash['offset'] < 0
        hash['payload'].force_encoding('BINARY')
        message = Peer::Message.new(sender_id, hash['senderSeqNo'], offset, nil, nil, hash['payload'])
        message_log_append(message)
      end
    end

    # Transforms the current peer's message_log array into a corresponding array of MessageLogEntry
    # records, for encoding to Avro.
    def encode_message_log
      message_log.map do |message|
        message.encoded ||= encode_message_payload(message)
        {
          'senderPeerIndex' => peer_matrix.peer_id_to_index(message.sender_id),
          'senderSeqNo'     => message.sender_seq_no,
          'offset'          => message.offset || -1,
          'payload'         => message.encoded
        }
      end
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

    # Parses an array of Avro CursorByPeer records, and applies them to the current peer.
    # It has the following structure:
    #
    #     [{
    #       # ItemID of the operation that created this map entry
    #       'id' => {'logicalTS' => 2, 'peerIndex' => 0},
    #
    #       # ItemID of the operation that last updated this map entry
    #       'updateTS' => {'logicalTS' => 234, 'peerIndex' => 0},
    #
    #       # PeerID (32-byte binary string)
    #       'key'   => '...',
    #
    #       # ItemID of character at current cursor position (may be null)
    #       'value' => {'logicalTS' => 123, 'peerIndex' => 3}
    #      }, ...]
    def decode_cursors(cursor_list)
      items = cursor_list.map do |cursor|
        Map::Item.new(
          decode_item_id(peer_id, cursor['id']),
          decode_item_id(peer_id, cursor['updateTS']),
          bin_to_hex(cursor['key']),
          decode_item_id(peer_id, cursor['value'])
        )
      end
      cursors.load_items(items)
    end

    # Takes the current peer's cursors information and transforms it into a list of Avro
    # CursorByPeer records.
    def encode_cursors
      cursors.items.map do |item|
        {
          'id'       => encode_item_id(item.put_id),
          'updateTS' => encode_item_id(item.update_ts),
          'key'      => hex_to_bin(item.key),
          'value'    => encode_item_id(item.value)
        }
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

      if message['payload'] # ReceiveMessage message
        message['payload'].force_encoding('BINARY')
        sender_id = bin_to_hex(message['senderID'])

        if bin_to_hex(message['channelID']) != channel_id
          raise "Received message on unexpected channel: #{bin_to_hex(message['channelID'])}"
        end

        if message['offset'] <= channel_offset
          raise "Non-monotonic channel offset: #{message.offset} <= #{channel_offset}"
        end
        self.channel_offset = message['offset']

        existing = (messages_by_sender[sender_id] || [])[message['senderSeqNo'] - 1]

        if existing
          # Message is already known to this peer (because we sent it ourselves, or because it is
          # a duplicate delivery), so just sanity-check it and record the offset
          if existing.offset && existing.offset != message['offset']
            raise "Mismatched message offset: #{existing.offset} != #{message['offset']}"
          end
          if existing.encoded != message['payload']
            raise "Mismatched message payload: #{bin_to_hex(existing.encoded)} != #{bin_to_hex(message['payload'])}"
          end
          if sender_id == peer_id
            logger.call "Received own message: seqNo=#{message['senderSeqNo']} --> offset=#{message['offset']}"
          else
            logger.call "Received duplicate message from #{sender_id}, seqNo=#{message['senderSeqNo']}"
          end
          existing.offset = message['offset']

        else
          # Message is not yet known to this peer, either because it came from someone else, or
          # because we sent it but crashed before persisting that fact to disk.
          msg_obj = decode_message_payload(sender_id, message)
          message_log_append(msg_obj)
          process_message(msg_obj)
          logger.call "Received message: seqNo=#{message['senderSeqNo']} senderId=#{sender_id} offset=#{message['offset']}"
        end

      elsif message['lastKnownSeqNo'] # SendMessageError
        replay_messages(message['lastKnownSeqNo'])
      else
        raise "Unexpected message type from server: #{message.inspect}"
      end
    end

    # Takes a Peer::Message object and constructs a SendMessage to send that message to a WebSocket
    # server. The SendMessage command has the following structure:
    #
    #     {
    #       # Identifies the channel on the server to which the message should be broadcast
    #       'channelID' => '...',
    #
    #       # Per-peerID, per-channel message sequence number (must increment by 1 for every message)
    #       'senderSeqNo' => 123,
    #
    #       # Avro-encoded, encrypted message payload as a byte string (not interpreted by the server)
    #       'payload' => '...'
    #     }
    def encode_message_request(message)
      message.encoded ||= encode_message_payload(message)
      message_hash = {
        'channelID'   => hex_to_bin(channel_id),
        'senderSeqNo' => message.sender_seq_no,
        'payload'     => message.encoded
      }

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
      writer = Avro::IO::DatumWriter.new(CLIENT_TO_SERVER_SCHEMA)
      writer.write({'message' => message_hash}, encoder)
      encoder.writer.string
    end

    # Returns a list of encoded SendMessage commands to send to a WebSocket server for all pending
    # messages. Resets the buffer of pending messages, so the same messages won't be returned again.
    def message_send_requests
      messages_to_send.map {|msg| encode_message_request(msg) }
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
      logger.call "Subscribing to channel #{channel_id} with startOffset #{channel_offset}"
      message_hash = {
        'channelID'   => hex_to_bin(channel_id),
        'startOffset' => channel_offset
      }

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
      writer = Avro::IO::DatumWriter.new(CLIENT_TO_SERVER_SCHEMA)
      writer.write({'message' => message_hash}, encoder)
      encoder.writer.string
    end

    # Decodes an incoming message from another peer, given as a decoded ReceiveMessage record (see
    # documentation for #receive_message). Returns a Peer::Message object.
    def decode_message_payload(sender_id, message)
      decrypted = secret_box ? secret_box.decrypt(message['payload']) : message['payload']
      decoder = Avro::IO::BinaryDecoder.new(StringIO.new(decrypted))
      reader = Avro::IO::DatumReader.new(MESSAGE_SCHEMA)
      payload = reader.read(decoder)

      operations = payload['operations'].map do |operation|
        if operation.include? 'updates' # ClockUpdate operation
          decode_clock_update(sender_id, operation)
        elsif operation.include? 'appSchema' # SchemaUpdate operation
          decode_schema_update(sender_id, operation)
        elsif operation.include? 'target' # Operation
          decode_operation(sender_id, operation)
        else
          raise "Unexpected operation type: #{operation.inspect}"
        end
      end

      # Decode schemaID after processing ClockUpdates, since the peerIndex mapping needs to be set
      # up first
      timestamp = Time.at(payload['timestamp'] / 1000.0)
      schema_id = decode_item_id(sender_id, payload['schemaID'])
      raise 'Unexpected schema ID' if default_schema_id && default_schema_id != schema_id

      Peer::Message.new(sender_id, message['senderSeqNo'], message['offset'], timestamp, operations, message['payload'])
    end

    # Transforms a CRDT::Peer::Message object into an Avro-encoded byte string that should be
    # broadcast to all peers.
    def encode_message_payload(message)
      operations = []

      unless message.operations.empty?
        # Always send a ClockUpdate as the first operation of a message, to establish the logical_ts
        # for subsequent operations. This isn't strictly necessary, as replicas should remember the
        # next_ts across messages, but it's a small amount of redundancy that doesn't cost much.
        unless message.operations.first.is_a? PeerMatrix::ClockUpdate
          message.operations.unshift(PeerMatrix::ClockUpdate.new)
        end

        if message.operations.size > 1
          next_ts = message.operations[1].op_id.logical_ts
        else
          next_ts = peer_matrix.next_ts_by_peer_id[peer_id]
        end
        message.operations.first.next_ts = next_ts

        message.operations.each do |operation|
          if operation.is_a? PeerMatrix::ClockUpdate
            operations << encode_clock_update(operation)
          else
            raise 'Operation not of local origin' if operation.op_id.peer_id != peer_id
            raise 'Non-monotonic logical timestamp' if operation.op_id.logical_ts < next_ts

            if operation.op_id.logical_ts > next_ts
              next_ts = operation.op_id.logical_ts
              operations << encode_clock_update(PeerMatrix::ClockUpdate.new(next_ts))
            end

            case operation
            when CRDT::SchemaUpdate then operations << encode_schema_update(operation)
            when CRDT::Operation    then operations << encode_operation(operation)
            else raise "Unexpected operation type: #{operation.inspect}"
            end

            next_ts += 1
          end
        end
      end

      message_hash = {
        'schemaID'   => encode_item_id(default_schema_id),
        'timestamp'  => (message.timestamp.to_f * 1000).round,
        'operations' => operations
      }

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new(''.force_encoding('BINARY')))
      Avro::IO::DatumWriter.new(MESSAGE_SCHEMA).write(message_hash, encoder)
      secret_box ? secret_box.encrypt(encoder.writer.string) : encoder.writer.string
    end

    # Decodes a ClockUpdate message from a remote peer. It has the following structure:
    #
    #     {
    #       # The counter portion of the logical timestamp to use for the next operation following
    #       # this clock update (subsequent operations increment by 1, until the next ClockUpdate).
    #       'nextTS' => 123,
    #
    #       'updates' => [
    #         # Assigns peerIndex = 3 to this peerID
    #         {'peerID' => 'asdfasdf', 'peerIndex' => 3, 'lastSeqNo' => 5},
    #
    #         # peerID can be omitted if peerIndex was previously assigned
    #         {'peerID' => nil, 'peerIndex' => 2, 'lastSeqNo' => 42},
    #         ...
    #       ]
    #     }
    def decode_clock_update(sender_id, operation)
      entries = operation['updates'].map do |entry|
        subject_peer_id = entry['peerID'] ? bin_to_hex(entry['peerID']) : nil

        # Need to register the peer index mapping right now, even though we don't apply the clock
        # update until later, because the peer index mapping may be needed to decode subsequent
        # operations.
        peer_matrix.peer_index_mapping(sender_id, subject_peer_id, entry['peerIndex'])

        PeerMatrix::PeerVClockEntry.new(subject_peer_id, entry['peerIndex'], entry['lastSeqNo'])
      end

      PeerMatrix::ClockUpdate.new(operation['nextTS'], entries)
    end

    # Encodes a CRDT::PeerMatrix::ClockUpdate operation for sending over the network.
    def encode_clock_update(clock_update)
      entries = clock_update.entries.map do |entry|
        {
          'peerID'    => entry.peer_id ? hex_to_bin(entry.peer_id) : nil,
          'peerIndex' => entry.peer_index,
          'lastSeqNo' => entry.last_seq_no
        }
      end
      {'nextTS' => clock_update.next_ts, 'updates' => entries}
    end

    # Decodes a SchemaUpdate message from a remote peer. It has the following structure:
    #
    #     {
    #       # Application version that generated this schema (for information/log messages only)
    #       'appVersion' => '1.0-beta3',
    #
    #       # JSON string with the schema defined by the application
    #       'appSchema' => '{"type":"record","name":"...",...}',
    #
    #       # JSON string with the schema used for encoding operations (derived from appSchema)
    #       'opSchema' => '{"type":"record","name":"...",...}'
    #     }
    def decode_schema_update(sender_id, operation)
      CRDT::SchemaUpdate.new(nil, operation['appVersion'], operation['appSchema'], operation['opSchema'])
    end

    # Encodes a CRDT::SchemaUpdate operation for sending over the network.
    def encode_schema_update(operation)
      {
        'appVersion' => operation.app_version.to_s,
        'appSchema'  => operation.app_schema.to_s,
        'opSchema'   => operation.op_schema.to_s
      }
    end

    # Decodes an Operation message from a remote peer. It has the following structure:
    #
    #     {
    #       # Identifies the object being modified in this operation
    #       'target' => {'logicalTS' => 123, 'peerIndex' => 3},
    #
    #       # Binary string, Avro-encoded using the opSchema specified in the message
    #       'operation' => '...'
    #     }
    def decode_operation(sender_id, operation)
      decoder = Avro::IO::BinaryDecoder.new(StringIO.new(operation['operation']))
      reader = Avro::IO::DatumReader.new(APP_OPERATION_SCHEMA)
      op_hash = reader.read(decoder)['operation']

      if op_hash.include? 'fieldNum' # InitializeRecordField operation
        app_op = InitializeRecordField.new(op_hash['fieldNum'])
      elsif op_hash.include? 'mapKey' # PutCursor operation
        app_op = Map::PutOp.new(bin_to_hex(op_hash['mapKey']),
                                decode_item_id(sender_id, op_hash['mapValue']))
      elsif op_hash.include? 'isSetCursor' # SetCursor operation
        app_op = Map::WriteOp.new(decode_item_id(sender_id, op_hash['registerValue']))
      elsif op_hash.include? 'referenceID' # InsertCharacter operation
        app_op = OrderedList::InsertOp.new(decode_item_id(sender_id, op_hash['referenceID']),
                                           op_hash['value'])
      elsif op_hash.include? 'isDeleteCharacter' # DeleteCharacter operation
        app_op = OrderedList::DeleteOp.new
      else
        raise "Unexpected operation type: #{op_hash.inspect}"
      end

      CRDT::Operation.new(nil, decode_item_id(sender_id, operation['target']), app_op)
    end

    # Encodes an Operation object for sending over the network.
    def encode_operation(operation)
      case operation.op
      when InitializeRecordField
        op_hash = {'fieldNum' => operation.op.field_num}
      when Map::PutOp
        op_hash = {
          'mapKey'   => hex_to_bin(operation.op.key),
          'mapValue' => encode_item_id(operation.op.value)
        }
      when Map::WriteOp
        op_hash = {
          'registerValue' => encode_item_id(operation.op.value),
          'isSetCursor'   => true
        }
      when OrderedList::InsertOp
        op_hash = {
          'referenceID' => encode_item_id(operation.op.reference_id),
          'value'       => operation.op.value
        }
      when OrderedList::DeleteOp
        op_hash = {'isDeleteCharacter' => true}
      else
        raise "Unexpected operation type: #{operation.inspect}"
      end

      encoder = Avro::IO::BinaryEncoder.new(StringIO.new(''.force_encoding('BINARY')))
      Avro::IO::DatumWriter.new(APP_OPERATION_SCHEMA).write({'operation' => op_hash}, encoder)
      {
        'target' => encode_item_id(operation.target),
        'operation' => encoder.writer.string
      }
    end

    # Translates an on-the-wire encoding of an ItemID (using a peer index) into an in-memory ItemID
    # object (using a peer ID).
    def decode_item_id(sender_id, hash)
      return nil if hash.nil?
      peer_id = peer_matrix.remote_index_to_peer_id(sender_id, hash['peerIndex'])
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
