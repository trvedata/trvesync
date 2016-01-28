package org.trvedata;

import java.util.Deque;

public class Message {
	private String originPeerId;
	private long msgCount;
	private Deque<Operation> operations;

	public Message(String originPeerId, long msgCount, Deque<Operation> operations) {
		this.originPeerId = originPeerId;
		this.msgCount = msgCount;
		this.operations = operations;
	}

	public Deque<Operation> getOperations() {
		return operations;
	}

	public long getMsgCount() {
		return msgCount;
	}

	public String getOriginPeerId() {
		return originPeerId;
	}

	@Override
	public String toString() {
		return "Message [originPeerId=" + originPeerId + ", msgCount=" + msgCount + ", operations=" + operations + "]";
	}
}