package org.trvedata.crdt;

import org.trvedata.crdt.operation.ChangingOperation;

public abstract class CRDT {
	
	protected Peer peer;
	
	public abstract void applyOperation(ChangingOperation changingOp);

	public Peer getPeer() {
		return peer;
	}

	void setPeer(Peer peer) {
		this.peer = peer;
	}
}
