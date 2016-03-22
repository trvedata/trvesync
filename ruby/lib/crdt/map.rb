module CRDT
  # A CRDT that maintains a mapping from keys to values. Each value is a register, so it can be
  # overwritten.
  class Map
    include Enumerable

    # Operation to represent adding a new key-value pair to a map. This is only used the first time
    # a key is inserted; subsequent updates to an existing key should use WriteOp.
    class PutOp < Struct.new(:header, :key, :value)
      def logical_ts; header.operation_id.logical_ts; end
    end

    # Operation to represent writing the value of a register, including updating the value of an
    # existing key-value pair in a map.
    class WriteOp < Struct.new(:header, :value)
      def logical_ts; header.operation_id.logical_ts; end
    end

    # Internal structure representing a key-value entry in the map.
    class Item < Struct.new(:put_id, :update_ts, :key, :value)
    end

    attr_reader :peer

    # Creates an empty map, running at the local +peer+.
    def initialize(peer)
      @peer = peer or raise ArgumentError, 'peer must be set'
      @items_by_key = {}
      @items_by_id = {}
    end

    # Returns a list of Item objects representing the internal structure of the map.
    def items
      @items_by_key.values
    end

    # Bulk loads an array of Item records (used to reload the data structure from disk).
    def load_items(items)
      items.each do |item|
        @items_by_key[item.key] = item
        @items_by_id[item.put_id] = item
      end
    end

    # Iterates over the map, calling the block with each key-value pair.
    def each(&block)
      items.each do |item|
        yield item.key, item.value
      end
    end

    # Looks up the value for the given key. Returns nil if the key is not present in the map.
    def [](key)
      item = @items_by_key[key]
      item.value if item
    end

    # Sets the given key to the given value (local operation). If the key already exists in the map,
    # it is treated as an update of the existing register; if the key is new, it is treated as an
    # insertion of a new key-value pair.
    def []=(key, value)
      item = @items_by_key[key]

      if item
        header = CRDT::OperationHeader.new(@peer.next_id, peer.default_schema_id, item.put_id, [])
        op = WriteOp.new(header, value)
      else
        # TODO fix hard-coded access path
        header = CRDT::OperationHeader.new(@peer.next_id, peer.default_schema_id, nil, [0])
        op = PutOp.new(header, key, value)
      end

      apply_operation(op)
      @peer.send_operation(op)
    end

    # Applies a remote operation to a local copy of the data structure.
    def apply_operation(operation)
      case operation
      when PutOp   then put_new_item(operation)
      when WriteOp then update_existing_item(operation)
      else raise "Invalid operation: #{operation}"
      end
    end

    private

    def put_new_item(op)
      raise 'PutOp is expected to be accessed from the root' if op.header.access_root
      raise "Unexpected access path: #{op.header.access_path.inspect}" if op.header.access_path != [0]
      existing_item = @items_by_key[op.key]

      if existing_item
        # Two concurrent puts for the same key occurred. Pick winner by logical timestamp.
        if op.header.operation_id > existing_item.update_ts
          raise 'Key mismatch for item' if op.key != existing_item.key
          existing_item.update_ts = op.header.operation_id
          existing_item.value = op.value
        end
      else
        item = Item.new(op.header.operation_id, op.header.operation_id, op.key, op.value)
        @items_by_key[op.key] = item
        @items_by_id[op.header.operation_id] = item
      end
    end

    def update_existing_item(op)
      raise 'WriteOp is expected to have access_root' if op.header.access_root.nil?
      raise "Unexpected access path: #{op.header.access_path.inspect}" if op.header.access_path != []

      item = @items_by_id[op.header.access_root]
      raise 'WriteOp references unknown item' if item.nil?

      if op.header.operation_id > item.update_ts
        item.update_ts = op.header.operation_id
        item.value = op.value
      end
    end
  end
end
