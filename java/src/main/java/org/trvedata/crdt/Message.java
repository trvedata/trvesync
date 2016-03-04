package org.trvedata.crdt;

import java.util.Deque;

import org.trvedata.crdt.operation.Operation;

public class Message {
	private PeerID originPeerId;
	private long msgCounter;
	private Deque<Operation> operations;

	public Message(PeerID originPeerId, long msgCount, Deque<Operation> operations) {
		this.originPeerId = originPeerId;
		this.msgCounter = msgCount;
		this.operations = operations;
	}

	public Deque<Operation> getOperations() {
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