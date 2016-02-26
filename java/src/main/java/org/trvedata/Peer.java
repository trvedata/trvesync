package org.trvedata;

import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class Peer<T> {
	private String peerId;
	private PeerMatrix peerMatrix;
	private OrderedList<T> orderedList;
	private long logicalTs;
	private Deque<Operation> sendBuf;
	private Map<String, Deque<Operation>> recvBuf;

	public Peer() {
		this(null);
	}

	public Peer(String peerId) {
		this.peerId = peerId != null ? peerId : this.createRandomPeerID();
		this.peerMatrix = new PeerMatrix(this.peerId);
		this.orderedList = new OrderedList<T>(this);
		this.logicalTs = 0;
		this.sendBuf = new ArrayDeque<Operation>();
		this.recvBuf = new HashMap<String, Deque<Operation>>();
	}

	protected String createRandomPeerID() {
		SecureRandom rand = new SecureRandom();
		byte[] bytes = new byte[32];
		String ret = "";
		rand.nextBytes(bytes);
		for (byte b : bytes)
			ret += String.format("%02x", b);
		return ret;
	}

	public boolean anythingToSend() {
		return !this.sendBuf.isEmpty();
	}

	protected ItemID nextId() {
		return new ItemID(++this.logicalTs, this.peerId);
	}

	protected void sendOperation(Operation operation) {
		this.sendClockUpdateIfNotEmpty();
		this.sendBuf.addLast(operation);
	}

	public Message makeMessage() {
		this.sendClockUpdateIfNotEmpty();
		Message message = new Message(this.peerId, this.peerMatrix.incrementMsgCount(), this.sendBuf);
		this.sendBuf = new ArrayDeque<Operation>();
		return message;
	}
	
	protected void sendClockUpdateIfNotEmpty() {
		if (!this.peerMatrix.getLocalClockUpdate().isEmpty()) {
			this.sendBuf.push(this.peerMatrix.getLocalClockUpdate());
			this.peerMatrix.resetClockUpdate();
		}
	}

	public void processMessage(Message message) {
		if (!(message instanceof Message))
			throw new RuntimeException("Invalid message: " + message);
		if (this.recvBuf.get(message.getOriginPeerId()) == null)
			this.recvBuf.put(message.getOriginPeerId(), new ArrayDeque<Operation>());
		// append all elements in message.operations to this.recvBuf
		this.recvBuf.get(message.getOriginPeerId()).addAll(message.getOperations());
		this.recvBuf.get(message.getOriginPeerId()).add(new MessageProcessed(message.getMsgCount()));
		while (this.applyOperationsIfReady())
			;
	}

	protected boolean applyOperationsIfReady() {
		String readyPeerId = null;
		Deque<Operation> readyOps = null;
		for (String peerId : this.recvBuf.keySet()) {
			if (this.peerMatrix.isCausallyReady(peerId) && !this.recvBuf.get(peerId).isEmpty()) {
				readyPeerId = peerId;
				readyOps = this.recvBuf.get(peerId);
				break;
			}
		}
		if (readyPeerId == null)
			return false;
		while (!readyOps.isEmpty()) {
			Operation operation = readyOps.pop();
			if (GlobalConstants.DEBUG)
				System.out.println("Peer " + peerId + ": Processing operation from " + readyPeerId + ": " + operation);
			if (operation instanceof ClockUpdate) {
				this.peerMatrix.applyClockUpdate(readyPeerId, (ClockUpdate) operation);
				// Applying the clock update might make the following operations causally non-ready, so we stop
				// processing operations from this peer and check again for causal readiness.
				return true;
			} else if (operation instanceof MessageProcessed) {
				MessageProcessed messageProcessed = (MessageProcessed) operation;
				this.peerMatrix.processedIncomingMsg(readyPeerId, messageProcessed.getMsgCount());
			} else {
				ChangingOperation changingOp = (ChangingOperation) operation;
				if (this.logicalTs < changingOp.logicalTs())
					this.logicalTs = changingOp.logicalTs();
				this.getOrderedList().applyOperation(changingOp);
			}
		}
		readyOps.clear();
		return true; // Finished this peer, now another peer's operations might be causally ready
	}

	public OrderedList<T> getOrderedList() {
		return orderedList;
	}

	public PeerMatrix getPeerMatrix() {
		return peerMatrix;
	}

	public String getPeerId() {
		return peerId;
	}

	@Override
	public String toString() {
		return "Peer [peerId=" + peerId + ", peerMatrix=" + peerMatrix + ", orderedList=" + orderedList + ", logicalTs=" + logicalTs + ", sendBuf="
				+ sendBuf + ", recvBuf=" + recvBuf + "]";
	}
}