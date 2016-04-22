package org.trvedata.crdt;

public class PeerID implements Comparable<PeerID> {
	
	final String peerID;

	public PeerID(String peerID) {
		if (peerID == null)
			throw new IllegalArgumentException("peerID must not be null");
		this.peerID = peerID;
	}

	public String getPeerID() {
		return peerID;
	}

	@Override
	public int hashCode() {
		return peerID.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PeerID other = (PeerID) obj;
		if (peerID == null) {
			if (other.peerID != null)
				return false;
		} else if (!peerID.equals(other.peerID))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PeerID [peerID=" + peerID + "]";
	}

	@Override
	public int compareTo(PeerID o) {
		return this.peerID.compareTo(o.peerID);
	}
}
