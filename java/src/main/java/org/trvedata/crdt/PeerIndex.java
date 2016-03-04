package org.trvedata.crdt;

public class PeerIndex {
	int idx;
	
	public PeerIndex(int idx) {
		this.idx = idx;
	}

	@Override
	public int hashCode() {
		return idx;
	}

	@Override
	public boolean equals(Object obj) {
		if (getClass() != obj.getClass())
			return false;
		PeerIndex other = (PeerIndex) obj;
		return idx == other.idx;
	}
}