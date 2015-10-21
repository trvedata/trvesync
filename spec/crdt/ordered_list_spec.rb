require 'crdt'

RSpec.describe CRDT::OrderedList do
  context '#to_a' do
    it 'should be empty by default' do
      list = CRDT::OrderedList.new(:site1)
      expect(list.to_a).to eq []
    end

    it 'should contain any inserted items' do
      list = CRDT::OrderedList.new(:site1)
      list.insert(0, :a).insert(1, :b).insert(0, :c)
      expect(list.to_a).to eq [:c, :a, :b]
    end

    it 'should omit any deleted items' do
      list = CRDT::OrderedList.new(:site1)
      list.insert(0, :a).insert(1, :b).delete(0)
      expect(list.to_a).to eq [:b]
    end
  end

  context '#flush_operations' do
    it 'should be empty by default' do
      list = CRDT::OrderedList.new(:site1)
      expect(list.flush_operations).to eq []
    end

    it 'should include details of an insert operation' do
      list = CRDT::OrderedList.new(:site1)
      list.insert(0, :a)
      expect(list.flush_operations).to eq [
        CRDT::OrderedList::Operation.new(
          :site1, {:site1 => 1}, :insert, nil,
          CRDT::Timestamp.new(:site1, 1), :a)
      ]
    end

    it 'should assign monotonically increasing clock values to operations' do
      list = CRDT::OrderedList.new(:site1)
      list.insert(0, :a).insert(1, :b).insert(2, :c).delete(1)
      ops = list.flush_operations
      expect(ops.map {|op| op.new_ts.clock }).to eq [1, 2, 3, 4]
      expect(ops.map {|op| op.vclock[:site1] }).to eq [1, 2, 3, 4]
      expect(ops.map {|op| op.value }).to eq [:a, :b, :c, nil]
    end

    it 'should reference prior inserts in later operations' do
      list = CRDT::OrderedList.new(:site1)
      list.insert(0, :a).insert(1, :b).insert(2, :c).delete(1)
      ops = list.flush_operations
      expect(ops[0].reference_ts).to eq nil
      expect(ops[1].reference_ts).to eq CRDT::Timestamp.new(:site1, 1)
      expect(ops[2].reference_ts).to eq CRDT::Timestamp.new(:site1, 2)
      expect(ops[3].reference_ts).to eq CRDT::Timestamp.new(:site1, 2)
    end

    it 'should include details of a delete operation' do
      list = CRDT::OrderedList.new(:site1)
      list.insert(0, :a).delete(0)
      expect(list.flush_operations.last).to eq (
        CRDT::OrderedList::Operation.new(
          :site1, {:site1 => 2}, :delete,
          CRDT::Timestamp.new(:site1, 1),
          CRDT::Timestamp.new(:site1, 2), nil)
      )
    end

    it 'should flush the operation list when called' do
      list = CRDT::OrderedList.new(:site1)
      list.insert(0, :a).delete(0)
      list.flush_operations
      expect(list.flush_operations).to eq []
    end
  end

  context '#apply_operations' do
    it 'should apply changes from another site' do
      site1 = CRDT::OrderedList.new(:site1)
      site2 = CRDT::OrderedList.new(:site2)
      site1.insert(0, :a).insert(1, :b).insert(2, :c).delete(1)
      site2.apply_operations(site1.flush_operations)
      expect(site2.to_a).to eq [:a, :c]
    end

    it 'should order concurrent inserts at the same position deterministically' do
      site1 = CRDT::OrderedList.new(:site1)
      site2 = CRDT::OrderedList.new(:site2)
      site1.insert(0, :a)
      site2.apply_operations(site1.flush_operations)
      site2.insert(1, :b)
      site1.insert(1, :c)
      site1.apply_operations(site2.flush_operations)
      site2.apply_operations(site1.flush_operations)
      expect(site1.to_a).to eq [:a, :b, :c]
      expect(site2.to_a).to eq [:a, :b, :c]
    end

    it 'should order concurrent inserts at the head deterministically' do
      site1 = CRDT::OrderedList.new(:site1)
      site2 = CRDT::OrderedList.new(:site2)
      site2.insert(0, :a).insert(1, :b)
      site1.insert(0, :c).insert(1, :d)
      site2.apply_operations(site1.flush_operations)
      site1.apply_operations(site2.flush_operations)
      expect(site1.to_a).to eq [:a, :b, :c, :d]
      expect(site2.to_a).to eq [:a, :b, :c, :d]
    end

    it 'should allow concurrent insertion and deletion at the same position' do
      site1 = CRDT::OrderedList.new(:site1)
      site2 = CRDT::OrderedList.new(:site2)
      site1.insert(0, :a)
      site2.apply_operations(site1.flush_operations)
      site1.delete(0)
      site2.insert(1, :b)
      site1.apply_operations(site2.flush_operations)
      site2.apply_operations(site1.flush_operations)
      expect(site1.to_a).to eq [:b]
      expect(site2.to_a).to eq [:b]
    end

    it 'should advance the local vector clock' do
      site1 = CRDT::OrderedList.new(:site1)
      site2 = CRDT::OrderedList.new(:site2)
      site1.insert(0, :a)
      site2.apply_operations(site1.flush_operations)
      expect(site2.vclock).to eq(site1: 1)

      site2.insert(1, :b)
      expect(site2.vclock).to eq(site1: 1, site2: 1)

      site1.insert(1, :c)
      site2.apply_operations(site1.flush_operations)
      expect(site2.vclock).to eq(site1: 2, site2: 1)
    end

    it 'should not generate any further operations' do
      site1 = CRDT::OrderedList.new(:site1)
      site2 = CRDT::OrderedList.new(:site2)
      site1.insert(0, :a)
      site2.apply_operations(site1.flush_operations)
      expect(site2.flush_operations).to eq []
    end
  end

  context '#causally_ready?' do
    it 'should check that dependencies are satisfied' do
      site1 = CRDT::OrderedList.new(:site1)
      site2 = CRDT::OrderedList.new(:site2)
      site3 = CRDT::OrderedList.new(:site3)
      site1.insert(0, :a)
      site1_op = site1.flush_operations.first

      site2.apply_operation(site1_op)
      site2.insert(1, :b)
      site2_op = site2.flush_operations.first

      expect(site3.causally_ready?(site1_op)).to be true
      expect(site3.causally_ready?(site2_op)).to be false
      site3.apply_operation(site1_op)
      expect(site3.causally_ready?(site2_op)).to be true
    end
  end
end
