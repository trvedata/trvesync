require 'crdt'

RSpec.describe CRDT::OrderedList do

  # make_peers(n) creates n peers on the same channel
  def make_peers(num_peers)
    peer0 = CRDT::Peer.new('peer0')
    [peer0] + (1...num_peers).map do |i|
      CRDT::Peer.new("peer#{i}", channel_id: peer0.channel_id)
    end
  end

  context '#to_a' do
    it 'should be empty by default' do
      peer = CRDT::Peer.new(:peer1)
      expect(peer.ordered_list.to_a).to eq []
    end

    it 'should contain any inserted items' do
      peer = CRDT::Peer.new(:peer1)
      peer.ordered_list.insert(0, :a).insert(1, :b).insert(0, :c)
      expect(peer.ordered_list.to_a).to eq [:c, :a, :b]
    end

    it 'should omit any deleted items' do
      peer = CRDT::Peer.new(:peer1)
      peer.ordered_list.insert(0, :a).insert(1, :b).delete(0)
      expect(peer.ordered_list.to_a).to eq [:b]
    end
  end

  context 'cursor editing' do
    before :each do
      @peer = CRDT::Peer.new(:peer1)
      @ids = [:a, :b, :c, :d, :e, :f].map {|item| @peer.ordered_list.insert_before_id(nil, item) }
    end

    it 'should allow insertion at a cursor position' do
      expect(@ids.first).to be_a(CRDT::ItemID)
      expect { @peer.ordered_list.insert_before_id(@ids[2], :x) }
        .to change { @peer.ordered_list.to_a }
        .from([:a, :b, :c, :d, :e, :f])
        .to([:a, :b, :x, :c, :d, :e, :f])
    end

    it 'should allow isnertion at the head' do
      expect { @peer.ordered_list.insert_before_id(@ids[0], :x) }
        .to change { @peer.ordered_list.to_a }
        .from([:a, :b, :c, :d, :e, :f])
        .to([:x, :a, :b, :c, :d, :e, :f])
    end

    it 'should allow deleting items before a cursor' do
      expect { @peer.ordered_list.delete_before_id(@ids[4], 2) }
        .to change { @peer.ordered_list.to_a }
        .from([:a, :b, :c, :d, :e, :f])
        .to([:a, :b, :e, :f])
    end

    it 'should allow deleting items after a cursor' do
      expect { @peer.ordered_list.delete_after_id(@ids[2], 2) }
        .to change { @peer.ordered_list.to_a }
        .from([:a, :b, :c, :d, :e, :f])
        .to([:a, :b, :e, :f])
    end

    it 'should allow deleting items from the tail' do
      expect { @peer.ordered_list.delete_before_id(nil, 1) }
        .to change { @peer.ordered_list.to_a }
        .from([:a, :b, :c, :d, :e, :f])
        .to([:a, :b, :c, :d, :e])
    end

    it 'should ignore deletes overrunning the head' do
      expect(@peer.ordered_list.delete_before_id(@ids[2], 4)).to be_nil
      expect(@peer.ordered_list.to_a).to eq [:c, :d, :e, :f]
    end

    it 'should ignore deletes overrunning the tail' do
      expect(@peer.ordered_list.delete_after_id(@ids[4], 4)).to be_nil
      expect(@peer.ordered_list.to_a).to eq [:a, :b, :c, :d]
    end

    it 'should skip over tombstones while deleting backwards' do
      expect(@peer.ordered_list.delete_after_id(@ids[2], 1)).to eq @ids[3]
      expect(@peer.ordered_list.delete_after_id(@ids[4], 1)).to eq @ids[5]
      expect(@peer.ordered_list.delete_before_id(@ids[5], 2)).to eq @ids[0]
      expect(@peer.ordered_list.to_a).to eq [:a, :f]
    end

    it 'should skip over tombstones while deleting forwards' do
      expect(@peer.ordered_list.delete_before_id(@ids[3], 1)).to eq @ids[1]
      expect(@peer.ordered_list.delete_before_id(@ids[5], 1)).to eq @ids[3]
      expect(@peer.ordered_list.delete_after_id(@ids[1], 2)).to eq @ids[5]
      expect(@peer.ordered_list.to_a).to eq [:a, :f]
    end
  end

  context 'generating operations' do
    it 'should include details of an insert operation' do
      peer = CRDT::Peer.new(:peer1)
      peer.make_message
      peer.ordered_list.insert(0, :a)
      expect(peer.make_message.operations).to eq [
        CRDT::OrderedList::InsertOp.new(
          CRDT::OperationHeader.new(CRDT::ItemID.new(2, :peer1), peer.default_schema_id, nil, [1]),
          nil, :a)
      ]
    end

    it 'should assign monotonically increasing clock values to operations' do
      peer = CRDT::Peer.new(:peer1)
      peer.make_message
      peer.ordered_list.insert(0, :a).insert(1, :b).insert(2, :c)
      ops = peer.make_message.operations
      expect(ops.map {|op| op.header.operation_id.logical_ts }).to eq [2, 3, 4]
      expect(ops.map {|op| op.value }).to eq [:a, :b, :c]
    end

    it 'should reference prior inserts in later operations' do
      peer = CRDT::Peer.new(:peer1)
      peer.make_message
      peer.ordered_list.insert(0, :a).insert(1, :b).insert(2, :c).delete(1)
      ops = peer.make_message.operations
      expect(ops[0].reference_id).to eq nil
      expect(ops[1].reference_id).to eq CRDT::ItemID.new(2, :peer1)
      expect(ops[2].reference_id).to eq CRDT::ItemID.new(3, :peer1)
      expect(ops[3].delete_id).to    eq CRDT::ItemID.new(3, :peer1)
    end

    it 'should include details of a delete operation' do
      peer = CRDT::Peer.new(:peer1)
      peer.make_message
      peer.ordered_list.insert(0, :a).delete(0)
      expect(peer.make_message.operations.last).to eq (
        CRDT::OrderedList::DeleteOp.new(
          CRDT::OperationHeader.new(CRDT::ItemID.new(3, :peer1), peer.default_schema_id, nil, [1]),
          CRDT::ItemID.new(2, :peer1))
      )
    end

    it 'should flush the operation list' do
      peer = CRDT::Peer.new(:peer1)
      peer.make_message
      peer.ordered_list.insert(0, :a).delete(0)
      peer.make_message
      expect(peer.make_message.operations).to eq []
    end
  end

  context 'applying remote operations' do
    it 'should apply changes from another peer' do
      peer1, peer2 = make_peers(2)
      peer1.ordered_list.insert(0, :a).insert(1, :b).insert(2, :c).delete(1)
      peer2.process_message(peer1.make_message)
      expect(peer2.ordered_list.to_a).to eq [:a, :c]
    end

    it 'should order concurrent inserts at the same position deterministically' do
      peer1, peer2 = make_peers(2)
      peer1.ordered_list.insert(0, :a)
      peer2.process_message(peer1.make_message)
      peer2.ordered_list.insert(1, :b)
      peer1.ordered_list.insert(1, :c)
      peer1.process_message(peer2.make_message)
      peer2.process_message(peer1.make_message)
      expect(peer1.ordered_list.to_a).to eq [:a, :b, :c]
      expect(peer2.ordered_list.to_a).to eq [:a, :b, :c]
    end

    it 'should order concurrent inserts at the head deterministically' do
      peer1, peer2 = make_peers(2)
      peer2.process_message(peer1.make_message)
      peer1.ordered_list.insert(0, :a).insert(1, :b)
      peer2.ordered_list.insert(0, :c).insert(1, :d)
      peer2.process_message(peer1.make_message)
      peer1.process_message(peer2.make_message)
      expect(peer1.ordered_list.to_a).to eq [:a, :b, :c, :d]
      expect(peer2.ordered_list.to_a).to eq [:a, :b, :c, :d]
    end

    it 'should allow concurrent insertion and deletion at the same position' do
      peer1, peer2 = make_peers(2)
      peer1.ordered_list.insert(0, :a)
      peer2.process_message(peer1.make_message)
      peer1.ordered_list.delete(0)
      peer2.ordered_list.insert(1, :b)
      peer1.process_message(peer2.make_message)
      peer2.process_message(peer1.make_message)
      expect(peer1.ordered_list.to_a).to eq [:b]
      expect(peer2.ordered_list.to_a).to eq [:b]
    end
  end

  context 'causality' do
    it 'should check that dependencies are satisfied' do
      peer1, peer2, peer3 = make_peers(3)
      peer1.ordered_list.insert(0, :a)
      peer1_msg = peer1.make_message

      peer2.process_message(peer1_msg)
      peer2.ordered_list.insert(1, :b)
      peer2_msg = peer2.make_message

      peer3.process_message(peer2_msg)
      expect(peer3.ordered_list.to_a).to eq []
      peer3.process_message(peer1_msg)
      expect(peer3.ordered_list.to_a).to eq [:a, :b]
    end
  end
end
