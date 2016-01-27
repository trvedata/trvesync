package org.trvedata;


// One entry in a vector clock. The +peerId+ is the hex string representing
// a peer; the peerIndex is the number we have
// locally assigned to that peer; and msgCount is the number of messages we
// have received from that peer.
public class PeerVClockEntry implements Comparable<PeerVClockEntry> {
	String peerId;
	int peerIndex;
	long msgCount;

	public PeerVClockEntry(String peerId, int peerIndex, long msgCount) {
		this.peerId = peerId;
		this.peerIndex = peerIndex;
		this.msgCount = msgCount;
	}

	public String toString() {
		return "PeerVClockEntry (peerId: " + this.peerId + ", peerIndex: " + this.peerIndex + ", msgCount: "
				+ this.msgCount + ")";
	}

	@Override
	public int compareTo(PeerVClockEntry o) {
		return this.peerIndex < o.peerIndex ? -1 : this.peerIndex == o.peerIndex ? 0 : 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (msgCount ^ (msgCount >>> 32));
		result = prime * result + ((peerId == null) ? 0 : peerId.hashCode());
		result = prime * result + peerIndex;
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
		if (peerIndex != other.peerIndex)
			return false;
		return true;
	}
}