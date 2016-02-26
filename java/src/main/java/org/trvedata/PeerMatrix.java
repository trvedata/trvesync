package org.trvedata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PeerMatrix {
	
	private static class PeerVClockList implements Iterable<PeerVClockEntry> {
		List<PeerVClockEntry> vClocks;

		public PeerVClockList(List<PeerVClockEntry> vClocks) {
			this.vClocks = new ArrayList<PeerVClockEntry>(vClocks);
		}

		public PeerVClockEntry getClockEntry(PeerIndex i) {
			return i.idx < vClocks.size() ? vClocks.get(i.idx) : null;
		}

		public int size() {
			return this.vClocks.size();
		}

		public boolean hasPeerId(String peerId) {
			for (PeerVClockEntry entry : this.vClocks)
				if (entry.getPeerId().equals(peerId))
					return true;
			return false;
		}

		public PeerVClockEntry setClockEntry(PeerIndex subjectPeerIndex, PeerVClockEntry entry) {
			while (this.vClocks.size() <= subjectPeerIndex.idx)
				this.vClocks.add(null);
			this.vClocks.set(subjectPeerIndex.idx, entry);
			return entry;
		}

		@Override
		public Iterator<PeerVClockEntry> iterator() {
			return this.vClocks.iterator();
		}

		public void add(PeerVClockEntry peerVClockEntry) {
			this.vClocks.add(peerVClockEntry);
		}

		@Override
		public String toString() {
			return "MatrixEntry [vClocks=" + vClocks + "]";
		}
	}
	
	private static final PeerIndex PEER_INDEX_LOCAL = new PeerIndex(0);
	
	private List<PeerVClockList> vectorClockMatrix;
	private HashMap<String, PeerIndex> indexByPeerId;
	private LocalClockUpdate localClockUpdate;

	public PeerMatrix(String ownPeerId) {
		// matrix.get(peer1Index).get(peer2Index) records how many operations peer1 has seen from peer2. peer1Index is
		// according to this peer's local index assignment (see indexByPeerId); peer2Index is according to peer1's index
		// assignment.
		this.vectorClockMatrix = new ArrayList<PeerVClockList>();
		this.vectorClockMatrix.add(new PeerVClockList(Arrays.asList(new PeerVClockEntry(ownPeerId, PEER_INDEX_LOCAL, 0))));
		// Key: peer ID (as hex string), Value: the index that this peer has locally assigned to that peer ID. The
		// indexes must be strictly sequential.
		this.indexByPeerId = new HashMap<String, PeerIndex>();
		this.indexByPeerId.put(ownPeerId, PEER_INDEX_LOCAL);
		// used to record any operations we see from other peers, so that we can broadcast vector clock diffs to others.
		this.localClockUpdate = new LocalClockUpdate();
	}

	/**
	 * Returns the peer ID (globally unique hex string) for the local device.
	 */
	public String ownPeerId() {
		return getPeerVClockList(PEER_INDEX_LOCAL).getClockEntry(PEER_INDEX_LOCAL).getPeerId();
	}
	
	protected PeerVClockList getPeerVClockList(PeerIndex peerIndex) {
		return this.vectorClockMatrix.get(peerIndex.idx);
	}
	
	protected void setPeerVClockList(PeerIndex peerIndex, PeerVClockList peerVClockList) {
		this.vectorClockMatrix.set(peerIndex.idx, peerVClockList);
	}
	
	private void addPeerVClockList(PeerVClockList peerVClockList) {
		this.vectorClockMatrix.add(peerVClockList);
	}

	/**
	 * When we get a message from originPeerId, it may refer to another peer by an integer index remotePeerIndex. This
	 * method translates remotePeerIndex (which is meaningful only in the context of messages from originPeerId) to the
	 * corresponding peer Id (a hex string that is globally unique).
	 */
	public String remoteIndexToPeerId(String originPeerId, PeerIndex remotePeerIndex) {
		PeerVClockEntry entry = getPeerVClockList(this.peerIdToIndex(originPeerId)).getClockEntry(remotePeerIndex);
		if (entry == null)
			throw new RuntimeException("remoteIndexToPeerId: No peer Id for index " + remotePeerIndex);
		return entry.getPeerId();
	}

	/**
	 * Translates a globally unique peer ID into a local peer index. If the peer ID is not already known, it is added to
	 * the matrix and assigned a new index.
	 */
	public PeerIndex peerIdToIndex(String peerId) {
		PeerIndex index = this.indexByPeerId.get(peerId);
		if (index != null)
			return index;
		index = new PeerIndex(this.indexByPeerId.size());
		
		assert index.idx == this.vectorClockMatrix.size();
		assert index.idx == this.vectorClockMatrix.get(PEER_INDEX_LOCAL.idx).size();
		assert !this.vectorClockMatrix.get(PEER_INDEX_LOCAL.idx).hasPeerId(peerId);
		
		this.indexByPeerId.put(peerId, index);
		getPeerVClockList(PEER_INDEX_LOCAL).add(new PeerVClockEntry(peerId, index, 0));
		addPeerVClockList(new PeerVClockList(Arrays.asList(new PeerVClockEntry(peerId, PEER_INDEX_LOCAL, 0))));
		this.localClockUpdate.addPeer(peerId, index);
		return index;
	}

	/*
	 * Indicates that the peer originPeerId has assigned an index of subjectPeerIndex to the peer subjectPeerId. Calling
	 * this method registers the mapping, so that subsequent calls to remoteIndexToPeerId can resolve the index. Returns
	 * the appropriate PeerVClockEntry.
	 */
	protected PeerVClockEntry peerIndexMapping(String originPeerId, String subjectPeerId, PeerIndex subjectPeerIndex) {
		PeerVClockList vclocks = getPeerVClockList(this.peerIdToIndex(originPeerId));
		PeerVClockEntry entry = vclocks.getClockEntry(subjectPeerIndex);
		if (entry != null) {
			if (subjectPeerId != null && subjectPeerId != entry.getPeerId())
				throw new RuntimeException("peerIndexMapping: Contradictory peer index assignment: " + subjectPeerId + " != " + entry.getPeerId());
			return entry;
		} else if (subjectPeerIndex.idx != vclocks.size()) {
			throw new RuntimeException("peerIndexMapping: Non-consecutive peer index assignment: " + subjectPeerIndex + " != " + vclocks.size());
		} else if (subjectPeerId == null) {
			throw new RuntimeException("peerIndexMapping: New peer index assignment without ID");
		} else {
			return vclocks.setClockEntry(subjectPeerIndex, new PeerVClockEntry(subjectPeerId, subjectPeerIndex, 0));
		}
	}

	/*
	 * Processes a clock update from a remote peer and applies it to the local state. The update indicates that
	 * originPeerId has received various operations from other peers, and also documents which peer indexes originPeerId
	 * has assigned to those peers.
	 */
	protected void applyClockUpdate(String originPeerId, ClockUpdate update) {
		for (PeerVClockEntry newEntry : update.entries()) {
			PeerVClockEntry oldEntry = this.peerIndexMapping(originPeerId, newEntry.getPeerId(), newEntry.getPeerIndex());
			if (oldEntry.getMsgCount() > newEntry.getMsgCount())
				throw new RuntimeException("applyClockUpdate: Clock update went backwards: " + oldEntry.getMsgCount() + " > " + newEntry.getMsgCount());
			oldEntry.setMsgCount(newEntry.getMsgCount());
		}
	}

	/*
	 * Increments the message counter for the local peer, indicating that a message has been broadcast to other peers.
	 */
	protected long incrementMsgCount() {
		return getPeerVClockList(PEER_INDEX_LOCAL).getClockEntry(PEER_INDEX_LOCAL).incrementMsgCount();
	}

	/*
	 * Increments the message counter for a particular peer, indicating that we have processed a message that originated
	 * on that peer. In other words, this moves the vector clock forward.
	 */
	protected void processedIncomingMsg(String originPeerId, long msgCount) {
		PeerIndex originIndex = this.peerIdToIndex(originPeerId);
		PeerVClockEntry localEntry = getPeerVClockList(PEER_INDEX_LOCAL).getClockEntry(originIndex);
		PeerVClockEntry remoteEntry = getPeerVClockList(originIndex).getClockEntry(PEER_INDEX_LOCAL);

		// We normally expect the msgCount for a peer to be monotonically increasing. However, there's a possible
		// scenario in which a peer sends some messages and then crashes before writing its state to stable storage, so
		// when it comes back up, it reverts back to a lower msgCount. We should detect when this happens, and replay
		// the lost messages from another peer.
		if (localEntry.getPeerId() != originPeerId)
			throw new RuntimeException("processedIncomingMsg: peerid mismatch: " + localEntry.getPeerId() + " != " + originPeerId);
		if (localEntry.getMsgCount() + 1 > msgCount)
			throw new RuntimeException("processedIncomingMsg: msgCount for " + originPeerId + " went backwards: " + localEntry.getMsgCount() + 1 + " > " + msgCount);
		if (localEntry.getMsgCount() + 1 < msgCount)
			throw new RuntimeException("processedIncomingMsg: msgCount for " + originPeerId + " jumped forwards: " + localEntry.getMsgCount() + 1 + " < " + msgCount);

		localEntry.setMsgCount(msgCount);
		remoteEntry.setMsgCount(msgCount);
		this.localClockUpdate.recordUpdate(originPeerId, originIndex, msgCount);
	}

	/**
	 * Returns <code>true</code> if operations originating on the given peer ID are ready to be delivered to the
	 * application, and false if they need to be buffered. Operations are causally ready if all operations they may
	 * depend on (which had been processed by the time that operation was generated) have already been applied locally.
	 * We assume that pairwise communication between peers is totally ordered, i.e. that messages from one particular
	 * peer are received in the same order as they were sent.
	 */
	public boolean isCausallyReady(String remotePeerId) {
		Map<String, Long> localVclock = new HashMap<String, Long>();
		for (PeerVClockEntry entry : getPeerVClockList(PEER_INDEX_LOCAL)) {
			localVclock.put(entry.getPeerId(), entry.getMsgCount());
		}

		Map<String, Long> remoteVclock = new HashMap<String, Long>();
		for (PeerVClockEntry entry : getPeerVClockList(this.peerIdToIndex(remotePeerId))) {
			remoteVclock.put(entry.getPeerId(), entry.getMsgCount());
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

	/*
	 * Resets the tracking of messages received from other peers. This is done after a clock update has been broadcast
	 * to other peers, so that we only transmit a diff of changes to the clock since the last clock update.
	 */
	protected void resetClockUpdate() {
		this.localClockUpdate = new LocalClockUpdate();
	}

	public LocalClockUpdate getLocalClockUpdate() {
		return localClockUpdate;
	}

	@Override
	public String toString() {
		return "PeerMatrix [matrix=" + vectorClockMatrix + ", indexByPeerId=" + indexByPeerId + ", localClockUpdate=" + localClockUpdate + "]";
	}
}