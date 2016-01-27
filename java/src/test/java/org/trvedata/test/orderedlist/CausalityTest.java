package org.trvedata.test.orderedlist;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.trvedata.Message;
import org.trvedata.Peer;

public class CausalityTest {
	@Test
	public void testDependenciesSatisfied() { //should check that dependencies are satisfied
	    Peer<Character> peer1 = new Peer<Character>("peer1");
	    Peer<Character> peer2 = new Peer<Character>("peer2");
	    Peer<Character> peer3 = new Peer<Character>("peer3");
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