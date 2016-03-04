package org.trvedata.crdt.operation;

public class MessageProcessed implements Operation {
	private long msgCounter;

	public MessageProcessed(long msgCounter) {
		this.msgCounter = msgCounter;
	}

	public long getMsgCounter() {
		return msgCounter;
	}

	@Override
	public String toString() {
		return "MessageProcessed [msgCount=" + msgCounter + "]";
	}
}