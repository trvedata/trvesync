package org.trvedata.crdt;

/**
 * One entry in a vector clock. The peerId is the hex string representing a peer; the peerIndex is the number we have
 * locally assigned to that peer; and msgCount is the number of messages we have received from that peer.
 */
public class PeerVClockEntry implements Comparable<PeerVClockEntry> {
	private String peerId;
	private PeerIndex peerIndex;
	private long msgCount;

	public PeerVClockEntry(String peerId, PeerIndex peerIndex, long msgCount) {
		this.peerId = peerId;
		this.peerIndex = peerIndex;
		this.setMsgCount(msgCount);
	}

	public String getPeerId() {
		return peerId;
	}

	public PeerIndex getPeerIndex() {
		return peerIndex;
	}

	public long incrementMsgCount() {
		return ++msgCount;
	}

	public long getMsgCount() {
		return msgCount;
	}

	public void setMsgCount(long msgCount) {
		this.msgCount = msgCount;
	}

	@Override
	public String toString() {
		return "PeerVClockEntry [peerId=" + peerId + ", peerIndex=" + peerIndex + ", msgCount=" + msgCount + "]";
	}

	@Override
	public int compareTo(PeerVClockEntry o) {
		return this.peerIndex.idx < o.peerIndex.idx ? -1 : this.peerIndex == o.peerIndex ? 0 : 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (msgCount ^ (msgCount >>> 32));
		result = prime * result + ((peerId == null) ? 0 : peerId.hashCode());
		result = prime * result + ((peerIndex == null) ? 0 : peerIndex.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PeerVClockEntry other = (PeerVClockEntry) obj;
		if (msgCount != other.msgCount)
			return false;
		if (peerId == null) {
			if (other.peerId != null)
				return false;
		} else if (!peerId.equals(other.peerId))
			return false;
		if (peerIndex == null) {
			if (other.peerIndex != null)
				return false;
		} else if (!peerIndex.equals(other.peerIndex))
			return false;
		return true;
	}
}