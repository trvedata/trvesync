package org.trvedata.crdt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.trvedata.crdt.operation.ClockUpdate;
import org.trvedata.crdt.operation.Operation;
import org.trvedata.crdt.operation.OperationList;
import org.trvedata.crdt.orderedlist.OrderedListPeer;

public class PeerMatrixTest {
	
	@Test
	public void testAssignSequentialMessageNumbers() {
        OrderedListPeer<Character> peer = new OrderedListPeer<Character>();
        peer.getOrderedList().insert(0, 'a');
        assertEquals(peer.makeMessage().getMsgCounter(), 1L);
        peer.getOrderedList().insert(1, 'b');
        assertEquals(peer.makeMessage().getMsgCounter(), 2L);
        peer.getOrderedList().insert(2, 'c').remove(0);
        assertEquals(peer.makeMessage().getMsgCounter(), 3L);
    }

	@Test
    public void testAssignPeerIndexesInOrderSeen() {
        OrderedListPeer<Character> local = new OrderedListPeer<Character>();
        List<PeerID> otherPeerIds = new ArrayList<PeerID>();
        for (char letter : new char[] {'a', 'b', 'c'}) {
            OrderedListPeer<Character> remote = new OrderedListPeer<Character>();
            remote.getOrderedList().insert(0, letter);
            local.processMessage(remote.makeMessage());
            otherPeerIds.add(remote.getPeerId());
        }

        assertEquals(local.getPeerMatrix().peerIdToIndex(local.getPeerId()), new PeerIndex(0));
        assertEquals(local.getPeerMatrix().peerIdToIndex(otherPeerIds.get(0)), new PeerIndex(1));
        assertEquals(local.getPeerMatrix().peerIdToIndex(otherPeerIds.get(1)), new PeerIndex(2));
        assertEquals(local.getPeerMatrix().peerIdToIndex(otherPeerIds.get(2)), new PeerIndex(3));
    }

    @Test
    public void testGenerateClockUpdateWhenMessagesReceived() {
    	OrderedListPeer<Character> local = new OrderedListPeer<Character>();
    	OrderedListPeer<Character> remote1 = new OrderedListPeer<Character>();
    	OrderedListPeer<Character> remote2 = new OrderedListPeer<Character>();
        remote1.getOrderedList().insert(0, 'a').insert(1, 'b');
        remote2.getOrderedList().insert(0, 'z');
        local.processMessage(remote1.makeMessage());
        local.processMessage(remote2.makeMessage());
        remote1.getOrderedList().insert(2, 'c');
        local.processMessage(remote1.makeMessage());

        OperationList messageOperations = local.makeMessage().getOperations();
        Assert.assertFalse(messageOperations.isEmpty());
        Operation clockUpdate = messageOperations.getFirst();
        assertEquals(clockUpdate instanceof ClockUpdate, true);
        assertEquals(((ClockUpdate)clockUpdate).entries(), Arrays.asList(
                                               new PeerVClockEntry(remote1.getPeerId(), new PeerIndex(1), 2),
                                               new PeerVClockEntry(remote2.getPeerId(), new PeerIndex(2), 1)
                                               ));
    }

    @Test
    public void testIncludePeerIdOnlyOnFirstClockUpdate() {
        OrderedListPeer<Character> local = new OrderedListPeer<Character>();
        OrderedListPeer<Character> remote1 = new OrderedListPeer<Character>();
        remote1.getOrderedList().insert(0, 'a');
        local.processMessage(remote1.makeMessage());
        assertEquals(((ClockUpdate)local.makeMessage().getOperations().getFirst()).entries(), Arrays.asList(new PeerVClockEntry(remote1.getPeerId(), new PeerIndex(1), 1)));

        OrderedListPeer<Character> remote2 = new OrderedListPeer<Character>();
        remote2.getOrderedList().insert(0, 'a');
        local.processMessage(remote2.makeMessage());
        assertEquals(((ClockUpdate)local.makeMessage().getOperations().getFirst()).entries(), Arrays.asList(new PeerVClockEntry(remote2.getPeerId(), new PeerIndex(2), 1)));

        remote1.getOrderedList().insert(0, 'a');
        local.processMessage(remote1.makeMessage());
        assertEquals(((ClockUpdate)local.makeMessage().getOperations().getFirst()).entries(), Arrays.asList(new PeerVClockEntry(null, new PeerIndex(1), 2)));
    }

    @Test
    public void testDecodeRemotePeerIndexes() {
        OrderedListPeer<Character> peer1 = new OrderedListPeer<Character>();
        OrderedListPeer<Character> peer2 = new OrderedListPeer<Character>();
        OrderedListPeer<Character> peer3 = new OrderedListPeer<Character>();
        peer1.getOrderedList().insert(0, 'a');
        Message msg1 = peer1.makeMessage();

        peer2.processMessage(msg1);
        peer2.getOrderedList().insert(1, 'b');
        Message msg2 = peer2.makeMessage();

        peer3.processMessage(msg2);
        peer3.processMessage(msg1);
        peer3.getOrderedList().insert(2, 'c');
        Message msg3 = peer3.makeMessage();

        peer1.processMessage(msg2);
        peer1.processMessage(msg3);
        peer2.processMessage(msg3);
        peer1.processMessage(peer2.makeMessage());
        
        PeerIndex pIdx0 = new PeerIndex(0);
        PeerIndex pIdx1 = new PeerIndex(1);
        PeerIndex pIdx2 = new PeerIndex(2);

        assertEquals(peer1.getPeerMatrix().remoteIndexToPeerId(peer2.getPeerId(), pIdx0), peer2.getPeerId());
        assertEquals(peer1.getPeerMatrix().remoteIndexToPeerId(peer2.getPeerId(), pIdx1), peer1.getPeerId());
        assertEquals(peer1.getPeerMatrix().remoteIndexToPeerId(peer2.getPeerId(), pIdx2), peer3.getPeerId());

        assertEquals(peer1.getPeerMatrix().remoteIndexToPeerId(peer3.getPeerId(), pIdx0), peer3.getPeerId());
        assertEquals(peer1.getPeerMatrix().remoteIndexToPeerId(peer3.getPeerId(), pIdx1), peer2.getPeerId());
        assertEquals(peer1.getPeerMatrix().remoteIndexToPeerId(peer3.getPeerId(), pIdx2), peer1.getPeerId());

        assertEquals(peer2.getPeerMatrix().remoteIndexToPeerId(peer1.getPeerId(), pIdx0), peer1.getPeerId());
        assertEquals(peer2.getPeerMatrix().remoteIndexToPeerId(peer3.getPeerId(), pIdx0), peer3.getPeerId());
        assertEquals(peer2.getPeerMatrix().remoteIndexToPeerId(peer3.getPeerId(), pIdx1), peer2.getPeerId());
        assertEquals(peer2.getPeerMatrix().remoteIndexToPeerId(peer3.getPeerId(), pIdx2), peer1.getPeerId());

        assertEquals(peer3.getPeerMatrix().remoteIndexToPeerId(peer1.getPeerId(), pIdx0), peer1.getPeerId());
        assertEquals(peer3.getPeerMatrix().remoteIndexToPeerId(peer2.getPeerId(), pIdx0), peer2.getPeerId());
        assertEquals(peer3.getPeerMatrix().remoteIndexToPeerId(peer2.getPeerId(), pIdx1), peer1.getPeerId());
    }
    
    @Test
    public void testTrackCausalDependenciesAcrossPeers() {
        OrderedListPeer<Character> peer1 = new OrderedListPeer<Character>();
        OrderedListPeer<Character> peer2 = new OrderedListPeer<Character>();
        OrderedListPeer<Character> peer3 = new OrderedListPeer<Character>();

        peer1.getOrderedList().insert(0, 'a');
        Message msg1 = peer1.makeMessage();

        peer2.processMessage(msg1);
        peer2.getOrderedList().insert(1, 'b');
        Message msg2 = peer2.makeMessage();

        peer3.processMessage(msg2);
        assertEquals(peer3.getPeerMatrix().isCausallyReady(peer2.getPeerId()), false);
        peer3.processMessage(msg1);
        assertEquals(peer3.getPeerMatrix().isCausallyReady(peer2.getPeerId()), true);
        assertEquals(peer3.getPeerMatrix().isCausallyReady(peer1.getPeerId()), true);
    }

    @Test
    public void testDontSendMessagesIndefinitelyAfterNoMoreChanges() {
        OrderedListPeer<Character> peer1 = new OrderedListPeer<Character>();
        OrderedListPeer<Character> peer2 = new OrderedListPeer<Character>();

        peer1.getOrderedList().insert(0, 'a');

        for (int i = 0; i < 5; i++) {  // 5 rounds should more than sufficient for peers to get into a stable state
            if (!(peer1.anythingToSend() || peer2.anythingToSend()))
                break;

            if (peer1.anythingToSend())
                peer2.processMessage(peer1.makeMessage());

            if (peer2.anythingToSend())
                peer1.processMessage(peer2.makeMessage());
        }

        assertEquals(peer1.anythingToSend() || peer2.anythingToSend(), false);
    }


}
