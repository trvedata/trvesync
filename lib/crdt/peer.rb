require 'yaml'
require 'avro'
require 'openssl'

module CRDT
  class Peer
    SCHEMAS_FILE = File.expand_path('schemas.yaml', File.dirname(__FILE__))

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

    def self.load(filename)
      Avro::DataFile.open(filename) do |io|
        io.each do |record|
          return new(record)
        end
      end
    end

    def initialize(state)
      @state = state
    end

    def peer_id
      @state['peers'][0]['peerID'].unpack('H*').first
    end

    def save(file)
      writer = Avro::IO::DatumWriter.new(PEER_STATE_SCHEMA)
      io = Avro::DataFile::Writer.new(file, writer, PEER_STATE_SCHEMA)
      io << @state
    ensure
      io.close
    end
  end
end
