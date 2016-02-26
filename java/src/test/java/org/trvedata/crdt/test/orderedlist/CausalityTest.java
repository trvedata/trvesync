package org.trvedata.crdt.test.orderedlist;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.trvedata.crdt.Message;
import org.trvedata.crdt.orderedlist.OrderedListPeer;

public class CausalityTest {
	@Test
	public void testDependenciesSatisfied() { // should check that dependencies are satisfied
		OrderedListPeer<Character> peer1 = new OrderedListPeer<Character>("peer1");
		OrderedListPeer<Character> peer2 = new OrderedListPeer<Character>("peer2");
		OrderedListPeer<Character> peer3 = new OrderedListPeer<Character>("peer3");
		peer1.getOrderedList().insert(0, 'a');
		Message peer1Msg = peer1.makeMessage();

		peer2.processMessage(peer1Msg);
		peer2.getOrderedList().insert(1, 'b');
		Message peer2Msg = peer2.makeMessage();

		peer3.processMessage(peer2Msg);
		assertEquals(peer3.getOrderedList().toList(), Collections.emptyList());
		peer3.processMessage(peer1Msg);
		assertEquals(peer3.getOrderedList().toList(), Arrays.asList('a', 'b'));
	}
}