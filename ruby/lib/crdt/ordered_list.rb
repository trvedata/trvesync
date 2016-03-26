module CRDT
  # A simple CRDT that represents an ordered list of items. It allows random-access insertion
  # or deletion of items. It is based on the "Replicated Growable Array" (RGA) datatype
  # described in the following paper:
  #
  # Hyun-Gul Roh, Myeongjae Jeon, Jin-Soo Kim, and Joonwon Lee.
  # Replicated abstract data types: Building blocks for collaborative applications.
  # Journal of Parallel and Distributed Computing, 71(3):354-368, March 2011.
  # http://dx.doi.org/10.1016/j.jpdc.2010.12.006
  # http://csl.skku.edu/papers/jpdc11.pdf
  class OrderedList
    include Enumerable

    # Operation to represent the insertion of an element into a list.
    InsertOp = Struct.new(:reference_id, :value)

    # Operation to represent the deletion of an element from a list.
    class DeleteOp
      def ==(other); other.is_a?(DeleteOp); end
    end

    # Internal structure representing a list element.
    Item = Struct.new(:insert_id, :delete_ts, :value, :previous, :next)

    attr_reader :peer

    # Creates an empty ordered list, running at the local +peer+.
    def initialize(peer)
      @peer = peer or raise ArgumentError, 'peer must be set'
      @items_by_id = {}
      @head = nil
      @tail = nil
    end

    # Iterates over the items in the list in order, skipping tombstones, and yields
    # each value of a list item to the +block+.
    def each(&block)
      item = @head
      while item
        yield item.value unless item.delete_ts
        item = item.next
      end
    end

    def each_item(&block)
      item = @head
      while item
        yield item
        item = item.next
      end
    end

    # Inserts a new item at the given +index+ in the list (local operation).
    # An index of 0 inserts at the head of the list; index out of bounds appends at the end.
    def insert(index, value)
      if index > 0
        right_item = item_by_index(index)
        left_id = right_item ? right_item.previous.insert_id : @tail.insert_id
      end
      item = insert_after_id(left_id, peer.next_id, value)

      raise 'characters_item_id not set' if peer.characters_item_id.nil?
      op = CRDT::Operation.new(item.insert_id, peer.characters_item_id,
                               InsertOp.new(item.previous && item.previous.insert_id, item.value))
      peer.send_operation(op)
      self
    end

    # Inserts a new item before the existing item identified by +cursor_id+ (local operation).
    # If +cursor_id+ is nil, appends to the end of the list.
    def insert_before_id(cursor_id, value)
      if cursor_id.nil?
        left_id = @tail && @tail.insert_id
      else
        right_item = @items_by_id[cursor_id] or raise IndexError
        left_id = right_item.previous && right_item.previous.insert_id
      end

      item = insert_after_id(left_id, peer.next_id, value)

      raise 'characters_item_id not set' if peer.characters_item_id.nil?
      op = CRDT::Operation.new(item.insert_id, peer.characters_item_id,
                               InsertOp.new(item.previous && item.previous.insert_id, item.value))
      peer.send_operation(op)
      item.insert_id
    end

    # Deletes the item at the given +index+ in the list (local operation).
    def delete(index)
      item = item_by_index(index) or raise IndexError
      item.delete_ts = peer.next_id
      item.value = nil

      peer.send_operation(CRDT::Operation.new(item.delete_ts, item.insert_id, DeleteOp.new))
      self
    end

    # Deletes +num_items+ items from the list (local operation). The items to be deleted are to the
    # left of the item identified by +cursor_id+ (not including the item identified by +cursor_id+
    # itself). If +cursor_id+ is nil, deletes +num_items+ from the end of the list. Returns the ID
    # of the last non-deleted item before the sequence of deleted items.
    def delete_before_id(cursor_id, num_items)
      if cursor_id.nil?
        item = @tail
      else
        cursor = @items_by_id[cursor_id] or raise IndexError
        item = cursor.previous
      end

      while item && (num_items > 0 || item.delete_ts)
        if item.delete_ts.nil?
          item.delete_ts = peer.next_id
          item.value = nil

          peer.send_operation(CRDT::Operation.new(item.delete_ts, item.insert_id, DeleteOp.new))
          num_items -= 1
        end

        item = item.previous
      end

      item && item.insert_id
    end

    # Deletes +num_items+ items from the list (local operation). The item identified by +cursor_id+
    # is the first item to be deleted, and the other deleted items are on its right. Returns the ID
    # of the first non-deleted item after the sequence of deleted items.
    def delete_after_id(cursor_id, num_items)
      item = @items_by_id[cursor_id] or raise IndexError

      while item && (num_items > 0 || item.delete_ts)
        if item.delete_ts.nil?
          item.delete_ts = peer.next_id
          item.value = nil

          peer.send_operation(CRDT::Operation.new(item.delete_ts, item.insert_id, DeleteOp.new))
          num_items -= 1
        end

        item = item.next
      end

      item && item.insert_id
    end

    # Applies a remote operation to a local copy of the data structure.
    # The operation must be causally ready, as per the data structure's vector clock.
    def apply_operation(operation)
      case operation.op
      when InsertOp
        raise "Unexpected target: #{operation.target.inspect}" if operation.target != peer.characters_item_id
        insert_after_id(operation.op.reference_id, operation.op_id, operation.op.value)
      when DeleteOp
        item = @items_by_id[operation.target] or raise IndexError
        item.delete_ts = operation.op_id
        item.value = nil
      else raise "Invalid operation: #{operation}"
      end
    end

    # Bulk loads an array of Item records (used to reload the data structure from disk).
    def load_items(items)
      if @head || @tail || !@items_by_id.empty?
        raise 'Cannot load into list that already contains data'
      end

      (0...items.size).each do |i|
        items[i].previous = items[i - 1] if i > 0
        items[i].next     = items[i + 1] if i < items.size - 1
        @items_by_id[items[i].insert_id] = items[i]
      end

      @head = items.first
      @tail = items.last
    end

    # Returns an array of Item records (used to save the data structure to disk).
    def dump_items
      items = []
      item = @head
      while item
        items << item
        item = item.next
      end
      items
    end

    private

    # Inserts a new list item to the right of the item identified by +left_id+.
    # If +left_id+ is nil, inserts a new list item at the head.
    # The new item has ID +insert_id+ and the given +value+.
    # Returns the newly inserted item.
    def insert_after_id(left_id, insert_id, value)
      if left_id
        left_item = @items_by_id[left_id] or raise IndexError
      elsif @head && @head.insert_id > insert_id
        left_item = @head
      end

      while left_item && left_item.next && left_item.next.insert_id > insert_id
        left_item = left_item.next
      end
      right_item = left_item ? left_item.next : @head

      item = Item.new(insert_id, nil, value, left_item, right_item)
      @items_by_id[insert_id] = item

      left_item.next = item if left_item
      right_item.previous = item if right_item
      @head = item if left_item.nil?
      @tail = item if right_item.nil?
      item
    end

    # Fetches the item with the given index in the list, skipping tombstones.
    # Returns nil if the index is out of range. FIXME: O(n) complexity.
    def item_by_index(index)
      item = @head
      while item && (item.delete_ts || index > 0)
        index -= 1 unless item.delete_ts
        item = item.next
      end
      item
    end
  end
end
