package org.trvedata.crdt.operation;

import java.util.List;

import org.trvedata.crdt.PeerVClockEntry;

public interface ClockUpdate extends Operation {
	public List<PeerVClockEntry> entries();
}