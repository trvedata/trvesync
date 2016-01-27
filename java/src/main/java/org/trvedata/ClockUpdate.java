package org.trvedata;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


// A clock update is a special kind of operation, which can be broadcast
// from one peer to other peers. When a
// ClockUpdate is sent, it reflects the messages received by the sender
// (i.e. which operations the sender has previously
// received from other peers). This is used to track the causal dependencies
// between operations. When building up
// locally, no argument is given. When received from a remote peer, the
// argument is an array of PeerV_clock_entry
// objects.
public class ClockUpdate implements Operation {
	private HashMap<String, PeerVClockEntry> updateByPeerId;

	public ClockUpdate() {
		// this.entries = entries; //TODO do we need this?
		// A hash, where the key is a peer ID (hex string) and the value is
		// a PeerVClockEntry object.
		this.updateByPeerId = new HashMap<String, PeerVClockEntry>();
	}

	void addPeer(String peerId, int peerIndex) {
		this.updateByPeerId.put(peerId, new PeerVClockEntry(peerId, peerIndex, 0));
	}

	void recordUpdate(String peerId, int peerIndex, long msgCount) {
		if (!this.updateByPeerId.containsKey(peerId))
			this.updateByPeerId.put(peerId, new PeerVClockEntry(null, peerIndex, 0));
		this.updateByPeerId.get(peerId).msgCount = msgCount;
	}

	boolean isEmpty() {
		return this.updateByPeerId.isEmpty();
	}

	public List<PeerVClockEntry> entries() {
		List<PeerVClockEntry> res = new ArrayList<PeerVClockEntry>(this.updateByPeerId.values());
		Collections.sort(res);
		return res;
	}

	public String toString() {
		return "ClockUpdate (" + this.updateByPeerId + ")";
	}
}