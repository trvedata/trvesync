package org.trvedata.crdt;

import java.util.List;

import org.trvedata.crdt.operation.ClockUpdate;

public class RemoteClockUpdate implements ClockUpdate {

	private List<PeerVClockEntry> entries;
	private long nextTimestamp;
	
	public RemoteClockUpdate(long nextTimestamp, List<PeerVClockEntry> entries) {
		this.nextTimestamp = nextTimestamp;
		this.entries = entries;
	}

	@Override
	public List<PeerVClockEntry> entries() {
		return entries;
	}
	
	public long getNextTimestamp() {
		return nextTimestamp;
	}
}
