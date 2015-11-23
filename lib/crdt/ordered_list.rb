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

    Operation = Struct.new(:origin, :vclock, :opcode, :reference_id, :new_id, :value)
    Item = Struct.new(:insert_id, :delete_id, :value, :previous, :next)

    attr_reader :peer_id, :vclock

    # Creates an empty ordered list for a given +peer_id+ (which must be unique).
    def initialize(peer_id)
      @peer_id = peer_id
      @vclock = Hash.new(0)
      @lamport_clock = 0
      @items_by_id = {}
      @head = nil
      @tail = nil
      @operations = []
    end

    # Iterates over the items in the list in order, skipping tombstones, and yields
    # each value of a list item to the +block+.
    def each(&block)
      item = @head
      while item
        yield item.value unless item.delete_id
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
      item = insert_after_id(left_id, next_id, value)
      @operations << Operation.new(peer_id, vclock.dup, :insert,
                                   item.previous && item.previous.insert_id,
                                   item.insert_id, item.value)
      self
    end

    # Deletes the item at the given +index+ in the list (local operation).
    def delete(index)
      item = item_by_index(index) or raise IndexError
      item.delete_id = next_id
      item.value = nil
      @operations << Operation.new(peer_id, vclock.dup, :delete,
                                   item.insert_id, item.delete_id, nil)
      self
    end

    # Returns +true+ if the causal dependencies of +operation+ have been satisfied,
    # so it is ready to be delivered to this site.
    def causally_ready?(operation)
      (self.vclock.keys | operation.vclock.keys).all? do |peer_id|
        if operation.origin == peer_id
          operation.vclock[peer_id] == self.vclock[peer_id] + 1
        else
          operation.vclock[peer_id] <= self.vclock[peer_id]
        end
      end
    end

    # Returns a list of operations that should be sent to remote sites.
    # Resets the list, so the same operations won't be returned again.
    def flush_operations
      return_ops = @operations
      @operations = []
      return_ops
    end

    # Applies a remote operation to a local copy of the data structure.
    # The operation must be causally ready, as per the data structure's vector clock.
    def apply_operation(operation)
      raise 'Operation is not ready to be applied' unless causally_ready?(operation)

      case operation.opcode
      when :insert
        insert_after_id(operation.reference_id, operation.new_id, operation.value)
      when :delete
        item = @items_by_id[operation.reference_id] or raise IndexError
        item.delete_id = operation.new_id
        item.value = nil
      else raise "Invalid operation: #{operation.opcode}"
      end

      # Lamport clock
      if @lamport_clock < operation.new_id.logical_ts
        @lamport_clock = operation.new_id.logical_ts
      end

      # Vector clock
      @vclock[operation.origin] += 1
    end

    def apply_operations(operations)
      operations.each {|op| apply_operation(op) }
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
      while item && (item.delete_id || index > 0)
        index -= 1 unless item.delete_id
        item = item.next
      end
      item
    end

    def next_id
      vclock[peer_id] += 1
      @lamport_clock += 1
      ::CRDT::ItemID.new(@lamport_clock, peer_id)
    end
  end
end
