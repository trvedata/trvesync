package org.trvedata.test.orderedlist;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.trvedata.ItemID;
import org.trvedata.Peer;

public class CursorEditingTest {
	Peer<Character> peer;
	ItemID[] ids;

	@Before
	public void setUp() {
	    peer = new Peer<Character>("peer1");
	    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f'};
	    ids = new ItemID[chars.length];
	    for (int i = 0; i < chars.length; i++)
	    	ids[i] = peer.getOrderedList().insertBeforeId(null, chars[i]);
		assertEquals(ids[0] instanceof ItemID, true);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('a', 'b', 'c', 'd', 'e', 'f'));
	}

	@Test
	public void testInsertionAtCursorPos() { //should allow insertion at a cursor position
		peer.getOrderedList().insertBeforeId(ids[2], 'x');
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('a', 'b', 'x', 'c', 'd', 'e', 'f'));
	}

	@Test
	public void testInsertionAtHead() { //should allow insertion at the head
		peer.getOrderedList().insertBeforeId(ids[0], 'x');
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('x', 'a', 'b', 'c', 'd', 'e', 'f'));
	}

	@Test
	public void testDeletionBeforeCursor() { //should allow deleting items before a cursor
		peer.getOrderedList().removeBeforeId(ids[4], 2);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('a', 'b', 'e', 'f'));
	}

	@Test
	public void testDeletionAfterCursor() { //should allow deleting items after a cursor
		peer.getOrderedList().removeAfterId(ids[2], 2);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('a', 'b', 'e', 'f'));
	}

	@Test
	public void testDeletionFromTail() { //should allow deleting items from the tail
		peer.getOrderedList().removeBeforeId(null, 1);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('a', 'b', 'c', 'd', 'e'));
	}

	@Test
	public void testIgnoreRemovesOverrunningHead() { //should ignore removes overrunning the head
		assertEquals(peer.getOrderedList().removeBeforeId(ids[2], 4), null);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('c', 'd', 'e', 'f'));
	}

	@Test
	public void testIgnoreRemovesOverrunningTail() { //should ignore removes overrunning the tail
		assertEquals(peer.getOrderedList().removeAfterId(ids[4], 4), null);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('a', 'b', 'c', 'd'));
	}

	@Test
	public void testSkipTombstonesDeletingBackwards() { //should skip over tombstones while deleting backwards
		assertEquals(peer.getOrderedList().removeAfterId(ids[2], 1), ids[3]);
		assertEquals(peer.getOrderedList().removeAfterId(ids[4], 1), ids[5]);
		assertEquals(peer.getOrderedList().removeBeforeId(ids[5], 2), ids[0]);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('a', 'f'));
	}

	@Test
	public void testSkipTombstonesDeletingForwards() { //should skip over tombstones while deleting forwards
		assertEquals(peer.getOrderedList().removeBeforeId(ids[3], 1), ids[1]);
		assertEquals(peer.getOrderedList().removeBeforeId(ids[5], 1), ids[3]);
		assertEquals(peer.getOrderedList().removeAfterId(ids[1], 2), ids[5]);
		assertEquals(peer.getOrderedList().toList(), Arrays.asList('a', 'f'));
	}
}