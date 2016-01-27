package org.trvedata;
import java.util.Deque;
import java.util.Queue;


public class Message {
	String originPeerId;
	long msgCount;
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
}