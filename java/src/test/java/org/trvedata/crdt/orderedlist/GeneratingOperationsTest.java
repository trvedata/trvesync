package org.trvedata.crdt.orderedlist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.trvedata.crdt.ItemID;
import org.trvedata.crdt.PeerID;
import org.trvedata.crdt.operation.Operation;
import org.trvedata.crdt.orderedlist.DeleteOp;
import org.trvedata.crdt.orderedlist.InsertOp;
import org.trvedata.crdt.orderedlist.OrderedListPeer;

public class GeneratingOperationsTest {
	@Test
	public void testDefaultEmpty() { 
		// should be empty by default
		OrderedListPeer<Character> peer = new OrderedListPeer<Character>("peer1");
		assertTrue(peer.makeMessage().getOperations().isEmpty());
	}

	@Test
	public void testInsertOperation() { 
		// should include details of an insert operation
		OrderedListPeer<Character> peer = new OrderedListPeer<Character>("peer1");
		peer.getOrderedList().insert(0, 'a');
		assertEquals(new ArrayList<Operation>(peer.makeMessage().getOperations()),
				Arrays.asList(new InsertOp<Character>(null, new ItemID(1, new PeerID("peer1")), 'a')));
	}

	@Test
	public void testAssignMonotonicallyIncreasingClockValues() { 
		// should assign monotonically increasing clock values to operations
		OrderedListPeer<Character> peer = new OrderedListPeer<Character>("peer1");
		peer.getOrderedList().insert(0, 'a').insert(1, 'b').insert(2, 'c');
		Collection<Operation> ops = peer.makeMessage().getOperations();
		ArrayList<Long> logicalTs = new ArrayList<Long>();
		ArrayList<Character> values = new ArrayList<Character>();
		for (Operation op : ops) {
			logicalTs.add(((InsertOp) op).getInsertId().getLogicalTs());
			values.add(((InsertOp<Character>) op).getValue());
		}
		assertEquals(logicalTs, Arrays.asList(1L, 2L, 3L));
		assertEquals(values, Arrays.asList('a', 'b', 'c'));
	}

	@Test
	public void testReferencePriorInsertsInLaterOperations() { 
		// should reference prior inserts in later operations
		OrderedListPeer<Character> peer = new OrderedListPeer<Character>("peer1");
		peer.getOrderedList().insert(0, 'a').insert(1, 'b').insert(2, 'c').remove(1);
		List<Operation> ops = new ArrayList(peer.makeMessage().getOperations());
		assertTrue(ops.get(0) instanceof InsertOp<?>);
		assertEquals(((InsertOp) (ops.get(0))).getReferenceId(), null);
		assertTrue(ops.get(1) instanceof InsertOp<?>);
		assertEquals(((InsertOp) (ops.get(1))).getReferenceId(), new ItemID(1, new PeerID("peer1")));
		assertTrue(ops.get(2) instanceof InsertOp<?>);
		assertEquals(((InsertOp) (ops.get(2))).getReferenceId(), new ItemID(2, new PeerID("peer1")));
		assertTrue(ops.get(3) instanceof DeleteOp);
		assertEquals(((DeleteOp) (ops.get(3))).getDeleteId(), new ItemID(2, new PeerID("peer1")));
	}

	@Test
	public void testIncludeDetailsOfRemoveOperation() { 
		// should include details of a remove operation
		OrderedListPeer<Character> peer = new OrderedListPeer<Character>("peer1");
		peer.getOrderedList().insert(0, 'a').remove(0);
		assertEquals(peer.makeMessage().getOperations().getLast(), new DeleteOp(new ItemID(1, new PeerID("peer1")), new ItemID(2, new PeerID("peer1"))));
	}

	@Test
	public void testFlushOperationList() { 
		// should flush the operation list
		OrderedListPeer<Character> peer = new OrderedListPeer<Character>("peer1");
		peer.getOrderedList().insert(0, 'a').remove(0);
		peer.makeMessage();
		assertTrue(peer.makeMessage().getOperations().isEmpty());
	}
}