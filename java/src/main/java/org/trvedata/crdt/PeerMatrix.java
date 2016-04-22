package org.trvedata.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.trvedata.crdt.operation.LocalClockUpdate;

public class PeerMatrix {
	
	private class PeerVClockList implements Iterable<PeerVClockEntry> {
		HashMap<PeerIndex,PeerVClockEntry> vClocks = new HashMap<PeerIndex, PeerVClockEntry>();

		public PeerVClockList(PeerVClockEntry ownVClock) {
			assert ownVClock.getPeerIndex().equals(PEER_INDEX_LOCAL);
			vClocks.put(ownVClock.getPeerIndex(), ownVClock);
		}
		
		public PeerVClockEntry getClockEntry(PeerIndex i) {
			return vClocks.get(i);
		}
		
		public PeerVClockEntry getOwnPeerVClockEntry() {
			return getClockEntry(PEER_INDEX_LOCAL);
		}

		public int size() {
			return this.vClocks.size();
		}

		public boolean hasPeerId(PeerID peerId) {
			for (PeerVClockEntry entry : vClocks.values())
				if (entry.getPeerId().equals(peerId))
					return true;
			return false;
		}

		public PeerVClockEntry setClockEntry(PeerIndex subjectPeerIndex, PeerVClockEntry entry) {
			if (!subjectPeerIndex.equals(entry.getPeerIndex())) {
				throw new AssertionError("subjectPeerIndex != entry.getPeerIndex(): " + subjectPeerIndex + " != " + entry.getPeerIndex());
			}
			this.vClocks.put(subjectPeerIndex, entry);
			return entry;
		}

		@Override
		public Iterator<PeerVClockEntry> iterator() {
			return this.vClocks.values().iterator();
		}

		public void add(PeerVClockEntry peerVClockEntry) {
			this.vClocks.put(peerVClockEntry.getPeerIndex(), peerVClockEntry);
		}

		@Override
		public String toString() {
			return "MatrixEntry [vClocks=" + vClocks + "]";
		}
	}
	
	private static final PeerIndex PEER_INDEX_LOCAL = new PeerIndex(0);

//	vectorClockMatrix.get(peer1Index).get(peer2Index) records how many operations peer1 has seen from peer2.
//	peer1Index is according to this peer's local index assignment (see indexByPeerId); peer2Index is according to
//	peer1's index assignment. 
	private final HashMap<PeerIndex,PeerVClockList> vectorClockMatrix = new HashMap<PeerIndex, PeerMatrix.PeerVClockList>();
//	Key: peer ID, Value: the index that this peer has locally assigned to that peer ID. The indexes must be strictly sequential.
	private final HashMap<PeerID, PeerIndex> indexByPeerId = new HashMap<PeerID, PeerIndex>();
//	used to record any operations we see from other peers, so that we can broadcast vector clock diffs to others.
	private LocalClockUpdate localClockUpdate;
	private Map<PeerID,Long> nextTimestampByPeerID = new HashMap<PeerID, Long>(); 


	public PeerMatrix(PeerID ownPeerId) {
		setPeerVClockList(PEER_INDEX_LOCAL, createNewPeerVClockListForPeer(ownPeerId));
		this.indexByPeerId.put(ownPeerId, PEER_INDEX_LOCAL);
		this.localClockUpdate = new LocalClockUpdate();
		nextTimestampByPeerID.put(ownPeerId, 0L);
	}

	private PeerVClockList createNewPeerVClockListForPeer(PeerID ownPeerId) {
		return new PeerVClockList(new PeerVClockEntry(ownPeerId, PEER_INDEX_LOCAL, 0));
	}

	/**
	 * Returns the peer ID (globally unique hex string) for the local device.
	 */
	public PeerID ownPeerId() {
		return getOwnPeerVClockList().getClockEntry(PEER_INDEX_LOCAL).getPeerId();
	}

	private PeerVClockList getOwnPeerVClockList() {
		return getPeerVClockList(PEER_INDEX_LOCAL);
	}
	
	protected PeerVClockList getPeerVClockList(PeerIndex peerIndex) {
		return this.vectorClockMatrix.get(peerIndex);
	}
	
	protected void setPeerVClockList(PeerIndex peerIndex, PeerVClockList peerVClockList) {
		this.vectorClockMatrix.put(peerIndex, peerVClockList);
	}
	
	/**
	 * When we get a message from originPeerId, it may refer to another peer by an integer index remotePeerIndex. This
	 * method translates remotePeerIndex (which is meaningful only in the context of messages from originPeerId) to the
	 * corresponding peer Id (a hex string that is globally unique).
	 */
	public PeerID remoteIndexToPeerId(PeerID originPeerId, PeerIndex remotePeerIndex) {
		PeerVClockEntry entry = getPeerVClockList(this.peerIdToIndex(originPeerId)).getClockEntry(remotePeerIndex);
		if (entry == null)
			throw new RuntimeException("remoteIndexToPeerId: No peer Id for index " + remotePeerIndex);
		return entry.getPeerId();
	}

	/**
	 * Translates a globally unique peer ID into a local peer index. If the peer ID is not already known, it is added to
	 * the matrix and assigned a new index.
	 */
	public PeerIndex peerIdToIndex(PeerID peerId) {
		PeerIndex peerIndex = this.indexByPeerId.get(peerId);
		if (peerIndex != null)
			return peerIndex;
		peerIndex = new PeerIndex(this.indexByPeerId.size());
		
		assert peerIndex.getIdx() == this.vectorClockMatrix.size();
		assert peerIndex.getIdx() == getOwnPeerVClockList().size();
		assert !getOwnPeerVClockList().hasPeerId(peerId);
		
		this.indexByPeerId.put(peerId, peerIndex);
		getOwnPeerVClockList().add(new PeerVClockEntry(peerId, peerIndex, 0));
		setPeerVClockList(peerIndex, createNewPeerVClockListForPeer(peerId));
		this.localClockUpdate.addPeer(peerId, peerIndex);
		return peerIndex;
	}

	/*
	 * Indicates that the peer originPeerId has assigned an index of subjectPeerIndex to the peer subjectPeerId. Calling
	 * this method registers the mapping, so that subsequent calls to remoteIndexToPeerId can resolve the index. Returns
	 * the appropriate PeerVClockEntry.
	 */
	protected PeerVClockEntry peerIndexMapping(PeerID originPeerId, PeerID subjectPeerId, PeerIndex subjectPeerIndex) {
		PeerVClockList vclocks = getPeerVClockList(this.peerIdToIndex(originPeerId));
		PeerVClockEntry entry = vclocks.getClockEntry(subjectPeerIndex);
		if (entry != null) {
			if (subjectPeerId != null && subjectPeerId != entry.getPeerId())
				throw new RuntimeException("peerIndexMapping: Contradictory peer index assignment: " + subjectPeerId + " != " + entry.getPeerId());
			return entry;
		} else if (subjectPeerIndex.getIdx() != vclocks.size()) {
			throw new RuntimeException("peerIndexMapping: Non-consecutive peer index assignment: " + subjectPeerIndex + " != " + vclocks.size());
		} else if (subjectPeerId == null) {
			throw new RuntimeException("peerIndexMapping: New peer index assignment without ID");
		} else {
			return vclocks.setClockEntry(subjectPeerIndex, new PeerVClockEntry(subjectPeerId, subjectPeerIndex, 0));
		}
	}

	public void updateNextTimestamp(PeerID peerID, long nextTimestamp) {
		if (nextTimestamp < 0)
			throw new IllegalArgumentException("nextTimestamp < 0 for peer " + peerID + ": " + nextTimestamp);
		Long currentNextTs = nextTimestampByPeerID.get(peerID);
		if (currentNextTs != null) {
			if (nextTimestamp < currentNextTs) {
				throw new IllegalArgumentException("Non-monotonic logical timestamp: " + currentNextTs + " -> " + nextTimestamp + " for peer " + peerID);
			}
		}
		nextTimestampByPeerID.put(peerID, nextTimestamp);
	}
	
	public ItemID nextOperationID(PeerID peerID) {
		long nextTs = getCurrentNextTimestamp(peerID);
		
		if (nextTs == Long.MAX_VALUE)
			throw new IllegalStateException("nextTs == Long.MAX_VALUE for peer ID " + peerID);

		nextTimestampByPeerID.put(peerID, nextTs + 1);
		return new ItemID(nextTs, peerID);
	}
	
	/*
	 * Processes a clock update from a remote peer and applies it to the local state. The update indicates that
	 * originPeerId has received various operations from other peers, and also documents which peer indexes originPeerId
	 * has assigned to those peers.
	 */
	protected void applyRemoteClockUpdate(PeerID originPeerId, RemoteClockUpdate update) {
		updateNextTimestamp(originPeerId, update.getNextTimestamp());
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
		return getOwnPeerVClockList().getOwnPeerVClockEntry().incrementMsgCount();
	}

	/*
	 * Increments the message counter for a particular peer, indicating that we have processed a message that originated
	 * on that peer. In other words, this moves the vector clock forward.
	 */
	protected void processedIncomingMsg(PeerID originPeerId, long msgCounter) {
		PeerIndex originIndex = this.peerIdToIndex(originPeerId);
		PeerVClockEntry localEntry = getOwnPeerVClockList().getClockEntry(originIndex);
		PeerVClockEntry remoteEntry = getPeerVClockList(originIndex).getOwnPeerVClockEntry();

		// We normally expect the msgCount for a peer to be monotonically increasing. However, there's a possible
		// scenario in which a peer sends some messages and then crashes before writing its state to stable storage, so
		// when it comes back up, it reverts back to a lower msgCount. We should detect when this happens, and replay
		// the lost messages from another peer.
		if (!localEntry.getPeerId().equals(originPeerId))
			throw new RuntimeException("processedIncomingMsg: peerid mismatch: " + localEntry.getPeerId() + " != " + originPeerId);
		if (msgCounter < localEntry.getMsgCount() + 1)
			throw new RuntimeException("processedIncomingMsg: msgCount for " + originPeerId + " went backwards: " + localEntry.getMsgCount() + 1 + " > " + msgCounter);
		if (msgCounter > localEntry.getMsgCount() + 1)
			throw new RuntimeException("processedIncomingMsg: msgCount for " + originPeerId + " jumped forwards: " + localEntry.getMsgCount() + 1 + " < " + msgCounter);

		localEntry.setMsgCount(msgCounter);
		remoteEntry.setMsgCount(msgCounter);
		this.localClockUpdate.recordUpdate(originPeerId, originIndex, msgCounter);
	}

	/**
	 * Returns <code>true</code> if operations originating on the given peer ID are ready to be delivered to the
	 * application, and false if they need to be buffered. Operations are causally ready if all operations they may
	 * depend on (which had been processed by the time that operation was generated) have already been applied locally.
	 * We assume that pairwise communication between peers is totally ordered, i.e. that messages from one particular
	 * peer are received in the same order as they were sent.
	 */
	public boolean isCausallyReady(PeerID remotePeerId) {
		Map<PeerID, Long> localVclock = new HashMap<PeerID, Long>();
		for (PeerVClockEntry entry : getOwnPeerVClockList()) {
			localVclock.put(entry.getPeerId(), entry.getMsgCount());
		}

		Map<PeerID, Long> remoteVclock = new HashMap<PeerID, Long>();
		for (PeerVClockEntry entry : getPeerVClockList(this.peerIdToIndex(remotePeerId))) {
			remoteVclock.put(entry.getPeerId(), entry.getMsgCount());
		}

		HashSet<PeerID> allPeerIds = new HashSet<PeerID>(localVclock.keySet());
		allPeerIds.addAll(remoteVclock.keySet());
		for (PeerID peerId : allPeerIds) {
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

	public long getCurrentNextTimestamp(PeerID peerID) {
		Long nextTs = nextTimestampByPeerID.get(peerID);
		return nextTs == null ? 0 : nextTs;
	}
}