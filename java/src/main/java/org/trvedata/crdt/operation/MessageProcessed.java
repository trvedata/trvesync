package org.trvedata.crdt.operation;

public class MessageProcessed implements Operation {
	private long msgCount;

	public MessageProcessed(long msgCount) {
		this.msgCount = msgCount;
	}

	public long getMsgCount() {
		return msgCount;
	}

	@Override
	public String toString() {
		return "MessageProcessed [msgCount=" + msgCount + "]";
	}
}