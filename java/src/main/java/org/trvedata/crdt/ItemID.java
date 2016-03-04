package org.trvedata.crdt;

public class ItemID implements Comparable<ItemID> {
	private long logicalTs;
	private PeerID peerId;

	public ItemID(long logicalTs, PeerID peerId) {
		this.logicalTs = logicalTs;
		this.peerId = peerId;
	}

	public int compareTo(ItemID other) {
		if (this.logicalTs < other.logicalTs)
			return -1;
		if (this.logicalTs > other.logicalTs)
			return 1;
		return this.peerId.compareTo(other.peerId);
	}

	public long getLogicalTs() {
		return logicalTs;
	}

	public void setLogicalTs(long logicalTs) {
		this.logicalTs = logicalTs;
	}

	@Override
	public boolean equals(Object obj) {
		ItemID o = (ItemID) obj;
		return this.logicalTs == o.logicalTs && this.peerId.equals(o.peerId);
	}

	@Override
	public int hashCode() {
		return (int) (this.logicalTs * 1337 + this.peerId.hashCode());
	}

	@Override
	public String toString() {
		return "ItemID [logicalTs=" + logicalTs + ", peerId=" + peerId + "]";
	}
}
