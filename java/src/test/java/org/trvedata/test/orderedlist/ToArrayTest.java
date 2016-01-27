package org.trvedata.test.orderedlist;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.trvedata.Peer;

public class ToArrayTest {
	@Test
	public void testDefaultEmpty() { //should be empty by default
	    Peer<Character> peer =  new Peer<Character>("peer1");
		assertEquals(peer.getOrderedList().toList(), Collections.emptyList());
	}

	@Test
	public void testContainsInsertedItems() { //should contain any inserted items
	    Peer<Character> peer =  new Peer<Character>("peer1");
		peer.getOrderedList().insert(0, 'a').insert(1, 'b').insert(0, 'c');
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('c', 'a', 'b'));
	}

	@Test
	public void testOmitRemovedItems() { //should omit any removed items
	    Peer<Character> peer =  new Peer<Character>("peer1");
		peer.getOrderedList().insert(0, 'a').insert(1, 'b').remove(0);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('b'));
	}
}