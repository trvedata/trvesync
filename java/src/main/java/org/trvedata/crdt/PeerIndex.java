package org.trvedata.crdt;

public class PeerIndex {
	private final long idx;
	
	public PeerIndex(long idx) {
		this.idx = idx;
	}
	
	public long getIdx() {
		return idx;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(idx);
	}

	@Override
	public boolean equals(Object obj) {
		if (getClass() != obj.getClass())
			return false;
		PeerIndex other = (PeerIndex) obj;
		return idx == other.idx;
	}

	@Override
	public String toString() {
		return "PeerIndex [idx=" + idx + "]";
	}

	public int compareTo(PeerIndex o) {
		return Long.compare(idx, o.idx);
	}
}