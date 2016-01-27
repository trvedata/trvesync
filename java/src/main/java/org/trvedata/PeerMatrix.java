package org.trvedata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PeerMatrix {

	static class MatrixEntry implements Iterable<PeerVClockEntry> {
		List<PeerVClockEntry> vClocks;

		public MatrixEntry(List<PeerVClockEntry> vClocks) {
			this.vClocks = new ArrayList<PeerVClockEntry>(vClocks);
		}

		public PeerVClockEntry get(int i) {
			return i < vClocks.size() ? vClocks.get(i) : null;
		}

		public int size() {
			return this.vClocks.size();
		}

		public boolean hasPeerId(String peerId) {
			for (PeerVClockEntry entry : this.vClocks)
				if (entry.peerId.equals(peerId))
					return true;
			return false;
		}

		public void set(int subjectPeerIndex, PeerVClockEntry entry) {
			while (this.vClocks.size() <= subjectPeerIndex)
				this.vClocks.add(null);
			this.vClocks.set(subjectPeerIndex, entry);
		}

		@Override
		public Iterator<PeerVClockEntry> iterator() {
			return this.vClocks.iterator();
		}

		public void add(PeerVClockEntry peerVClockEntry) {
			this.vClocks.add(peerVClockEntry);
		}
	}

	private List<MatrixEntry> matrix;
	ClockUpdate localClockUpdate;
	private HashMap<String, Integer> indexByPeerId;

	public PeerMatrix(String ownPeerId) {
		// matrix is an array of MatrixEntry objects.
		// matrix[peer1Index][peer2Index] is a PeerVClockEntry object. Each
		// such object records how many operations peer1 has seen from
		// peer2. peer1Index is according to this peer's local
		// index assignment (see indexByPeerId); peer2Index is according to
		// peer1's index assignment.
		this.matrix = new ArrayList<MatrixEntry>();
		// this.matrix = [[new PeerVClockEntry(ownPeerId, 0, 0)]];
		this.matrix.add(new MatrixEntry(Arrays.asList(new PeerVClockEntry(ownPeerId, 0, 0))));

		// A hash, where the key is a peer ID (as hex string) and the value
		// is the index that this peer has locally assigned to
		// that peer ID. The indexes must be strictly sequential.
		this.indexByPeerId = new HashMap<String, Integer>();
		this.indexByPeerId.put(ownPeerId, 0);

		// This is used to record any operations we see from other peers, so
		// that we can broadcast vector clock diffs to others.
		this.localClockUpdate = new ClockUpdate();
	}

	// The peer ID (globally unique hex string) for the local device.
	String ownPeerId() {
		return this.matrix.get(0).get(0).peerId;
	}

	// When we get a message from originPeerId, it may refer to another
	// peer by an integer index remotePeerIndex. This
	// method translates remotePeerIndex (which is meaningful only in the
	// context of messages from originPeerId) to the
	// corresponding peer Id (a hex string that is globally unique).
	public String remoteIndexToPeerId(String originPeerId, int remotePeerIndex) {
		PeerVClockEntry entry = this.matrix.get(this.peerIdToIndex(originPeerId)).get(remotePeerIndex);
		if (entry != null)
			return entry.peerId;
		throw new RuntimeException("remoteIndexToPeerId: No peer Id for index " + remotePeerIndex);
	}

	// Translates a globally unique peer ID into a local peer index. If the
	// peer ID is not already known, it is added to the
	// matrix and assigned a new index.
	public int peerIdToIndex(String peerId) {
		Integer index = this.indexByPeerId.get(peerId);
		if (index != null) {
			return index;
		}

		index = this.indexByPeerId.size();
		assert index == this.matrix.size();
		assert index == this.matrix.get(0).size();
		assert !this.matrix.get(0).hasPeerId(peerId);

		this.indexByPeerId.put(peerId, index);
		this.matrix.get(0).add(new PeerVClockEntry(peerId, index, 0));
		this.matrix.add(new MatrixEntry(Arrays.asList(new PeerVClockEntry(peerId, 0, 0))));
		this.localClockUpdate.addPeer(peerId, index);
		return index;
	}

	// Indicates that the peer originPeerId has assigned an index of
	// subjectPeerIndex to the peer subjectPeerId.
	// Calling this method registers the mapping, so that subsequent calls
	// to remoteIndexToPeerId can resolve the index.
	// Returns the appropriate PeerVClockEntry.
	PeerVClockEntry peerIndexMapping(String originPeerId, String subjectPeerId, int subjectPeerIndex) {
		MatrixEntry vclocks = this.matrix.get(this.peerIdToIndex(originPeerId));
		PeerVClockEntry entry = vclocks.get(subjectPeerIndex);

		if (entry != null) {
			if (subjectPeerId != null && subjectPeerId != entry.peerId) {
				throw new RuntimeException("peerIndexMapping: Contradictory peer index assignment");
			}
			return entry;
		} else if (subjectPeerIndex != vclocks.size()) {
			throw new RuntimeException("peerIndexMapping: Non-consecutive peer index assignment");
		} else if (subjectPeerId == null) {
			throw new RuntimeException("peerIndexMapping: New peer index assignment without Id");
		}

		entry = new PeerVClockEntry(subjectPeerId, subjectPeerIndex, 0);
		vclocks.set(subjectPeerIndex, entry);
		return entry;
	}

	// Processes a clock update from a remote peer and applies it to the
	// local state. The update indicates that
	// originPeerId has received various operations from other peers, and
	// also documents which peer indexes originPeerId
	// has assigned to those peers.
	void applyClockUpdate(String originPeerId, ClockUpdate update) {
		for (PeerVClockEntry newEntry : update.entries()) {
			PeerVClockEntry oldEntry = this.peerIndexMapping(originPeerId, newEntry.peerId, newEntry.peerIndex);
			if (oldEntry.msgCount > newEntry.msgCount) {
				throw new RuntimeException("applyClockUpdate: Clock update went backwards");
			}
			oldEntry.msgCount = newEntry.msgCount;
		}
	}

	// Increments the message counter for the local peer, indicating that a
	// message has been broadcast to other peers.
	long getMessageCount(boolean increment) {
		if (increment)
			this.matrix.get(0).get(0).msgCount++;
		return this.matrix.get(0).get(0).msgCount;
	}

	// Increments the message counter for a particular peer, indicating that
	// we have processed a message that originated on
	// that peer. In other words, this moves the vector clock forward.
	void processedIncomingMsg(String originPeerId, long msgCount) {
		int originIndex = this.peerIdToIndex(originPeerId);
		PeerVClockEntry localEntry = this.matrix.get(0).get(originIndex);
		PeerVClockEntry remoteEntry = this.matrix.get(originIndex).get(0);

		// We normally expect the msgCount for a peer to be monotonically
		// increasing. However, there's a possible scenario in
		// which a peer sends some messages and then crashes before writing
		// its state to stable storage, so when it comes back
		// up, it reverts back to a lower msgCount. We should detect when
		// this happens, and replay the lost messages from
		// another peer.
		if (localEntry.peerId != originPeerId) {
			throw new RuntimeException("processedIncomingMsg: peerid mismatch: " + localEntry.peerId + " != "
					+ originPeerId);
		}
		if (localEntry.msgCount + 1 > msgCount) {
			throw new RuntimeException("processedIncomingMsg: msgCount for " + originPeerId + " went backwards");
		}
		if (localEntry.msgCount + 1 < msgCount) {
			throw new RuntimeException("processedIncomingMsg: msgCount for " + originPeerId + " jumped forwards");
		}

		localEntry.msgCount = msgCount;
		remoteEntry.msgCount = msgCount;

		this.localClockUpdate.recordUpdate(originPeerId, originIndex, msgCount);
	}

	// Returns true if operations originating on the given peer ID are ready
	// to be delivered to the application, and false
	// if they need to be buffered. Operations are causally ready if all
	// operations they may depend on (which had been
	// processed by the time that operation was generated) have already been
	// applied locally. We assume that pairwise
	// communication between peers is totally ordered, i.e. that messages
	// from one particular peer are received in the same
	// order as they were sent.
	public boolean isCausallyReady(String remotePeerId) {
		Map<String, Long> localVclock = new HashMap<String, Long>();
		for (PeerVClockEntry entry : this.matrix.get(0)) {
			localVclock.put(entry.peerId, entry.msgCount);
		}

		Map<String, Long> remoteVclock = new HashMap<String, Long>();
		for (PeerVClockEntry entry : this.matrix.get(this.peerIdToIndex(remotePeerId))) {
			remoteVclock.put(entry.peerId, entry.msgCount);
		}

		HashSet<String> allPeerIds = new HashSet<String>(localVclock.keySet());
		allPeerIds.addAll(remoteVclock.keySet());
		for (String peerId : allPeerIds) {
			Long local = localVclock.get(peerId);
			Long remote = remoteVclock.get(peerId);
			if ((!peerId.equals(remotePeerId)) && (local == null ? 0L : local) < (remote == null ? 0L : remote))
				return false;
		}
		return true;
	}

	// Resets the tracking of messages received from other peers. This is
	// done after a clock update has been broadcast to
	// other peers, so that we only transmit a diff of changes to the clock
	// since the last clock update.
	void resetClockUpdate() {
		this.localClockUpdate = new ClockUpdate();
	}
}