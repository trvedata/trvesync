package org.trvedata;

import java.util.List;

public interface ClockUpdate extends Operation {
	public List<PeerVClockEntry> entries();
}