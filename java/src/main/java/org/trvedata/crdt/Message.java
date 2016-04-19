package org.trvedata.crdt;

import org.trvedata.crdt.operation.OperationList;

public class Message {
	private PeerID originPeerId;
	private long msgCounter;
	private OperationList operations;

	public Message(PeerID originPeerId, long msgCount, OperationList operations) {
		this.originPeerId = originPeerId;
		this.msgCounter = msgCount;
		this.operations = operations;
	}

	public OperationList getOperations() {
		return operations;
	}

	public long getMsgCounter() {
		return msgCounter;
	}

	public PeerID getOriginPeerId() {
		return originPeerId;
	}

	@Override
	public String toString() {
		return "Message [originPeerId=" + originPeerId + ", msgCount=" + msgCounter + ", operations=" + operations + "]";
	}
}