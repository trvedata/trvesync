package org.trvedata.crdt.test.orderedlist;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.trvedata.crdt.orderedlist.OrderedListPeer;

public class ToArrayTest {
	@Test
	public void testDefaultEmpty() { // should be empty by default
		OrderedListPeer<Character> peer = new OrderedListPeer<Character>("peer1");
		assertEquals(peer.getOrderedList().toList(), Collections.emptyList());
	}

	@Test
	public void testContainsInsertedItems() { // should contain any inserted items
		OrderedListPeer<Character> peer = new OrderedListPeer<Character>("peer1");
		peer.getOrderedList().insert(0, 'a').insert(1, 'b').insert(0, 'c');
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('c', 'a', 'b'));
	}

	@Test
	public void testOmitRemovedItems() { // should omit any removed items
		OrderedListPeer<Character> peer = new OrderedListPeer<Character>("peer1");
		peer.getOrderedList().insert(0, 'a').insert(1, 'b').remove(0);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('b'));
	}
}