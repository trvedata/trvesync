
describe("OrderedList", function() {
    "use strict";

	describe('#to_a', function() {
		it('should be empty by default', function() {
		    var peer = new Peer('peer1');
			expect(Array.from(peer.orderedList)).toEqual([]);
		});

		it('should contain any inserted items', function() {
		    var peer = new Peer('peer1');
			peer.orderedList.insert(0, 'a').insert(1, 'b').insert(0, 'c');
			expect(Array.from(peer.orderedList)).toEqual(['c', 'a', 'b']);
		});

		it('should omit any removed items', function() {
		    var peer = new Peer('peer1');
			peer.orderedList.insert(0, 'a').insert(1, 'b').remove(0);
			expect(Array.from(peer.orderedList)).toEqual(['b']);
		});
	});

	describe('cursor editing', function() {
		var peer, ids;

		beforeEach(function() {
		    peer = new Peer('peer1');
		    ids = ['a', 'b', 'c', 'd', 'e', 'f'].map(function(item) {return peer.orderedList.insertBeforeId(null, item);});
			expect(ids[0] instanceof ItemID).toBe(true);
			expect(Array.from(peer.orderedList)).toEqual(['a', 'b', 'c', 'd', 'e', 'f']);
		});

		it('should allow insertion at a cursor position', function() {
			peer.orderedList.insertBeforeId(ids[2], 'x');
			expect(Array.from(peer.orderedList)).toEqual(['a', 'b', 'x', 'c', 'd', 'e', 'f']);
		});

		it('should allow isnertion at the head', function() {
			peer.orderedList.insertBeforeId(ids[0], 'x');
			expect(Array.from(peer.orderedList)).toEqual(['x', 'a', 'b', 'c', 'd', 'e', 'f']);
		});

		it('should allow deleting items before a cursor', function() {
			peer.orderedList.removeBeforeId(ids[4], 2);
			expect(Array.from(peer.orderedList)).toEqual(['a', 'b', 'e', 'f']);
		});

		it('should allow deleting items after a cursor', function() {
			peer.orderedList.removeAfterId(ids[2], 2);
			expect(Array.from(peer.orderedList)).toEqual(['a', 'b', 'e', 'f']);
		});

		it('should allow deleting items from the tail', function() {
			peer.orderedList.removeBeforeId(null, 1);
			expect(Array.from(peer.orderedList)).toEqual(['a', 'b', 'c', 'd', 'e']);
		});

		it('should ignore removes overrunning the head', function() {
			expect(peer.orderedList.removeBeforeId(ids[2], 4)).toBe(null);
			expect(Array.from(peer.orderedList)).toEqual(['c', 'd', 'e', 'f']);
		});

		it('should ignore removes overrunning the tail', function() {
			expect(peer.orderedList.removeAfterId(ids[4], 4)).toBe(null);
			expect(Array.from(peer.orderedList)).toEqual(['a', 'b', 'c', 'd']);
		});

		it('should skip over tombstones while deleting backwards', function() {
			expect(peer.orderedList.removeAfterId(ids[2], 1)).toEqual(ids[3]);
			expect(peer.orderedList.removeAfterId(ids[4], 1)).toEqual(ids[5]);
			expect(peer.orderedList.removeBeforeId(ids[5], 2)).toEqual(ids[0]);
			expect(Array.from(peer.orderedList)).toEqual(['a', 'f']);
		});

		it('should skip over tombstones while deleting forwards', function() {
			expect(peer.orderedList.removeBeforeId(ids[3], 1)).toEqual(ids[1]);
			expect(peer.orderedList.removeBeforeId(ids[5], 1)).toEqual(ids[3]);
			expect(peer.orderedList.removeAfterId(ids[1], 2)).toEqual(ids[5]);
			expect(Array.from(peer.orderedList)).toEqual(['a', 'f']);
		});
	});

	describe('generating operations',  function() {
		it('should be empty by default', function() {
		    var peer = new Peer('peer1');
			expect(peer.makeMessage().operations).toEqual([]);
		});

		it('should include details of an insert operation', function() {
		    var peer = new Peer('peer1');
			peer.orderedList.insert(0, 'a');
			expect(peer.makeMessage().operations).toEqual([new InsertOp(null, new ItemID(1, 'peer1'), 'a')]);
		});

		it('should assign monotonically increasing clock values to operations', function() {
		    var peer = new Peer('peer1');
			peer.orderedList.insert(0, 'a').insert(1, 'b').insert(2, 'c');
			var ops = peer.makeMessage().operations;
			expect(ops.map(function(op) {return op.newId.logicalTs; })).toEqual([1, 2, 3]);
			expect(ops.map(function(op) {return op.value; })).toEqual(['a', 'b', 'c']);
		});

		it('should reference prior inserts in later operations', function() {
		    var peer = new Peer('peer1');
			peer.orderedList.insert(0, 'a').insert(1, 'b').insert(2, 'c').remove(1);
			var ops = peer.makeMessage().operations;
			expect(ops[0].referenceId).toBe(null);
			expect(ops[1].referenceId).toEqual(new ItemID(1, 'peer1'));
			expect(ops[2].referenceId).toEqual(new ItemID(2, 'peer1'));
			expect(ops[3].deleteId).toEqual(new ItemID(2, 'peer1'));
		});

		it('should include details of a remove operation', function() {
		    var peer = new Peer('peer1');
			peer.orderedList.insert(0, 'a').remove(0);
			expect(peer.makeMessage().operations.pop()).toEqual(new DeleteOp(new ItemID(1, 'peer1'), new ItemID(2, 'peer1')));
		});

		it('should flush the operation list', function() {
		    var peer = new Peer('peer1');
			peer.orderedList.insert(0, 'a').remove(0);
			peer.makeMessage();
			expect(peer.makeMessage().operations).toEqual([]);
		});
	});

	describe('applying remote operations',  function() {
		it('should apply changes from another peer', function() {
		    var peer1 = new Peer('peer1');
			var peer2 = new Peer('peer2');
			peer1.orderedList.insert(0, 'a').insert(1, 'b').insert(2, 'c').remove(1);
			peer2.processMessage(peer1.makeMessage());
			expect(Array.from(peer2.orderedList)).toEqual(['a', 'c']);
		});

		it('should order concurrent inserts at the same position deterministically', function() {
		    var peer1 = new Peer('peer1');
		    var peer2 = new Peer('peer2');
		    peer1.orderedList.insert(0, 'a');
			peer2.processMessage(peer1.makeMessage());
			peer2.orderedList.insert(1, 'b');
			peer1.orderedList.insert(1, 'c');
			peer1.processMessage(peer2.makeMessage());
			peer2.processMessage(peer1.makeMessage());
			expect(Array.from(peer1.orderedList)).toEqual(['a', 'b', 'c']);
			expect(Array.from(peer2.orderedList)).toEqual(['a', 'b', 'c']);
		});

		it('should order concurrent inserts at the head deterministically', function() {
			var peer1 = new Peer('peer1');
			var peer2 = new Peer('peer2');
			peer2.orderedList.insert(0, 'a').insert(1, 'b');
			peer1.orderedList.insert(0, 'c').insert(1, 'd');
			peer2.processMessage(peer1.makeMessage());
			peer1.processMessage(peer2.makeMessage());
			expect(Array.from(peer1.orderedList)).toEqual(['a', 'b', 'c', 'd']);
			expect(Array.from(peer2.orderedList)).toEqual(['a', 'b', 'c', 'd']);
		});

		it('should allow concurrent insertion and deletion at the same position', function() {
			var peer1 = new Peer('peer1');
			var peer2 = new Peer('peer2');
			peer1.orderedList.insert(0, 'a');
			peer2.processMessage(peer1.makeMessage());
			peer1.orderedList.remove(0);
			peer2.orderedList.insert(1, 'b');
			peer1.processMessage(peer2.makeMessage());
			peer2.processMessage(peer1.makeMessage());
			expect(Array.from(peer1.orderedList)).toEqual(['b']);
			expect(Array.from(peer2.orderedList)).toEqual(['b']);
		});
	});

	describe('causality',  function() {
		it('should check that dependencies are satisfied', function() {
		    var peer1 = new Peer('peer1');
		    var peer2 = new Peer('peer2');
		    var peer3 = new Peer('peer3');
			peer1.orderedList.insert(0, 'a');
			var peer1Msg = peer1.makeMessage();

			peer2.processMessage(peer1Msg);
			peer2.orderedList.insert(1, 'b');
			var peer2Msg = peer2.makeMessage();

			peer3.processMessage(peer2Msg);
			expect(Array.from(peer3.orderedList)).toEqual([]);
			peer3.processMessage(peer1Msg);
			expect(Array.from(peer3.orderedList)).toEqual(['a', 'b']);
		});
	});
});
