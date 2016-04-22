package org.trvedata.crdt.operation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.trvedata.crdt.PeerID;
import org.trvedata.crdt.PeerIndex;
import org.trvedata.crdt.PeerVClockEntry;

/**
 * A clock update is a special kind of operation, which can be broadcast from one peer to other peers. When a
 * ClockUpdate is sent, it reflects the messages received by the sender (i.e. which operations the sender has previously
 * received from other peers). This is used to track the causal dependencies between operations.
 */
public class LocalClockUpdate implements ClockUpdate {
	private HashMap<PeerID, PeerVClockEntry> updateByPeerId = new HashMap<PeerID, PeerVClockEntry>();

	public LocalClockUpdate() {
	}

	public void addPeer(PeerID peerId, PeerIndex peerIndex) {
		this.updateByPeerId.put(peerId, new PeerVClockEntry(peerId, peerIndex, 0));
	}

	public void recordUpdate(PeerID peerId, PeerIndex peerIndex, long msgCount) {
		if (!this.updateByPeerId.containsKey(peerId))
			this.updateByPeerId.put(peerId, new PeerVClockEntry(null, peerIndex, 0));
		this.updateByPeerId.get(peerId).setMsgCount(msgCount);
	}

	public boolean isEmpty() {
		return this.updateByPeerId.isEmpty();
	}

	@Override
	public List<PeerVClockEntry> entries() {
		List<PeerVClockEntry> res = new ArrayList<PeerVClockEntry>(this.updateByPeerId.values());
		Collections.sort(res, new Comparator<PeerVClockEntry>() {
			@Override
			public int compare(PeerVClockEntry o1, PeerVClockEntry o2) {
				return o1.getPeerIndex().compareTo(o2.getPeerIndex());
			}
		});
		return res;
	}

	@Override
	public String toString() {
		return "LocalClockUpdate [updateByPeerId=" + updateByPeerId + "]";
	}
}