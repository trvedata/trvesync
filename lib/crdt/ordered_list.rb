module CRDT
  class Timestamp < Struct.new(:site_id, :clock)
    include Comparable

    def <=>(other)
      return +1 if self.clock > other.clock
      return -1 if self.clock < other.clock
      self.site_id <=> other.site_id
    end
  end

  class OrderedList < Site
    include Enumerable

    Operation = Struct.new(:origin, :vclock, :opcode, :reference_ts, :new_ts, :value)
    Item = Struct.new(:insert_ts, :delete_ts, :value, :previous, :next)

    # Creates an empty ordered list for a given +site_id+ (which must be unique).
    def initialize(site_id)
      super
      @lamport_clock = 0
      @items_by_ts = {}
      @head = nil
      @tail = nil
      @operations = []
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

    # Inserts a new item at the given +index+ in the list (local operation).
    # An index of 0 inserts at the head of the list; index out of bounds appends at the end.
    def insert(index, value)
      if index > 0
        right_item = item_by_index(index)
        left_ts = right_item ? right_item.previous.insert_ts : @tail.insert_ts
      end
      item = insert_after_ts(left_ts, next_ts, value)
      @operations << Operation.new(site_id, vclock.dup, :insert,
                                   item.previous && item.previous.insert_ts,
                                   item.insert_ts, item.value)
      self
    end

    # Deletes the item at the given +index+ in the list (local operation).
    def delete(index)
      item = item_by_index(index) or raise IndexError
      item.delete_ts = next_ts
      item.value = nil
      @operations << Operation.new(site_id, vclock.dup, :delete,
                                   item.insert_ts, item.delete_ts, nil)
      self
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
        insert_after_ts(operation.reference_ts, operation.new_ts, operation.value)
      when :delete
        item = @items_by_ts[operation.reference_ts] or raise IndexError
        item.delete_ts = operation.new_ts
        item.value = nil
      else raise "Invalid operation: #{operation.opcode}"
      end

      # Lamport clock
      if @lamport_clock < operation.new_ts.clock
        @lamport_clock = operation.new_ts.clock
      end

      # Vector clock
      @vclock[operation.origin] += 1
    end

    def apply_operations(operations)
      operations.each {|op| apply_operation(op) }
    end

    private

    # Inserts a new list item to the right of the item identified by +left_ts+.
    # If +left_ts+ is nil, inserts a new list item at the head.
    # The new item has timestamp +insert_ts+ and the given +value+.
    # Returns the newly inserted item.
    def insert_after_ts(left_ts, insert_ts, value)
      if left_ts
        left_item = @items_by_ts[left_ts] or raise IndexError
      elsif @head && @head.insert_ts > insert_ts
        left_item = @head
      end

      while left_item && left_item.next && left_item.next.insert_ts > insert_ts
        left_item = left_item.next
      end
      right_item = left_item ? left_item.next : @head

      item = Item.new(insert_ts, nil, value, left_item, right_item)
      @items_by_ts[insert_ts] = item

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

    def next_ts
      vclock[site_id] += 1
      @lamport_clock += 1
      ::CRDT::Timestamp.new(site_id, @lamport_clock)
    end
  end
end
