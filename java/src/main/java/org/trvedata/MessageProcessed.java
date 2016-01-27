package org.trvedata;

public class MessageProcessed implements Operation {
	long msgCount;

	public MessageProcessed(long msgCount) {
		this.msgCount = msgCount;
	}

	public String toString() {
		return "MessageProcessed (msgCount: " + this.msgCount + ")";
	}
}