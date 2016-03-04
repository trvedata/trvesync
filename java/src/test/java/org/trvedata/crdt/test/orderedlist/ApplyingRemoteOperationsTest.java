package org.trvedata.crdt.test.orderedlist;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.trvedata.crdt.Peer;
import org.trvedata.crdt.orderedlist.OrderedListPeer;

public class ApplyingRemoteOperationsTest {
	@Test
	public void testApplyChangesToOtherPeer() { //should apply changes from another peer
		OrderedListPeer<Character> peer1 = new OrderedListPeer<Character>("peer1");
		OrderedListPeer<Character> peer2 = new OrderedListPeer<Character>("peer2");
		peer1.getOrderedList().insert(0, 'a').insert(1, 'b').insert(2, 'c').remove(1);
		peer2.processMessage(peer1.makeMessage());
		assertEquals(peer2.getOrderedList().toList(), Arrays.asList('a', 'c'));
	}

	@Test
	public void testOrderConcurrentInsertsDeterministically() { //should order concurrent inserts at the same position deterministically
	    OrderedListPeer<Character> peer1 = new OrderedListPeer<Character>("peer1");
	    OrderedListPeer<Character> peer2 = new OrderedListPeer<Character>("peer2");
	    peer1.getOrderedList().insert(0, 'a');
		peer2.processMessage(peer1.makeMessage());
		peer2.getOrderedList().insert(1, 'b');
		peer1.getOrderedList().insert(1, 'c');
		peer1.processMessage(peer2.makeMessage());
		peer2.processMessage(peer1.makeMessage());
		assertEquals(peer1.getOrderedList().toList(), Arrays.asList('a', 'b', 'c'));
		assertEquals(peer2.getOrderedList().toList(), Arrays.asList('a', 'b', 'c'));
	}

	@Test
	public void testOrderConcurrentInsertsHeadDeterministically() { //should order concurrent inserts at the head deterministically
		OrderedListPeer<Character> peer1 = new OrderedListPeer<Character>("peer1");
		OrderedListPeer<Character> peer2 = new OrderedListPeer<Character>("peer2");
		peer2.getOrderedList().insert(0, 'a').insert(1, 'b');
		peer1.getOrderedList().insert(0, 'c').insert(1, 'd');
		peer2.processMessage(peer1.makeMessage());
		peer1.processMessage(peer2.makeMessage());
		assertEquals(peer1.getOrderedList().toList(), Arrays.asList('a', 'b', 'c', 'd'));
		assertEquals(peer2.getOrderedList().toList(), Arrays.asList('a', 'b', 'c', 'd'));
	}

	@Test
	public void testAllowConcurrentInsertDeleteSamePosition() { //should allow concurrent insertion and deletion at the same position
		OrderedListPeer<Character> peer1 = new OrderedListPeer<Character>("peer1");
		OrderedListPeer<Character> peer2 = new OrderedListPeer<Character>("peer2");
		peer1.getOrderedList().insert(0, 'a');
		peer2.processMessage(peer1.makeMessage());
		peer1.getOrderedList().remove(0);
		peer2.getOrderedList().insert(1, 'b');
		peer1.processMessage(peer2.makeMessage());
		peer2.processMessage(peer1.makeMessage());
		assertEquals(peer1.getOrderedList().toList(), Arrays.asList('b'));
		assertEquals(peer2.getOrderedList().toList(), Arrays.asList('b'));
	}
}