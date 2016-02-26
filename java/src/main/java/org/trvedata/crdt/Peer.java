package org.trvedata.crdt;

import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.trvedata.crdt.operation.ChangingOperation;
import org.trvedata.crdt.operation.ClockUpdate;
import org.trvedata.crdt.operation.LocalClockUpdate;
import org.trvedata.crdt.operation.MessageProcessed;
import org.trvedata.crdt.operation.Operation;
import org.trvedata.crdt.orderedlist.OrderedList;

public class Peer {
	
	private PeerID peerId;
	private PeerMatrix peerMatrix;
	private CRDT crdt;
	private long logicalTs;
	private Deque<Operation> sendBuf;
	private Map<PeerID, Deque<Operation>> recvBuf;

	public Peer() {
		this(null, null);
	}
	
	public Peer(String peerId, CRDT crdt) {
		this.peerId = peerId != null ? new PeerID(peerId) : this.createRandomPeerID();
		this.peerMatrix = new PeerMatrix(this.peerId);
		this.crdt = crdt != null ? crdt : new OrderedList();
		this.crdt.setPeer(this);
		this.logicalTs = 0;
		this.sendBuf = new ArrayDeque<Operation>();
		this.recvBuf = new HashMap<PeerID, Deque<Operation>>();
	}

	protected PeerID createRandomPeerID() {
		SecureRandom rand = new SecureRandom();
		byte[] bytes = new byte[32];
		String ret = "";
		rand.nextBytes(bytes);
		for (byte b : bytes)
			ret += String.format("%02x", b);
		return new PeerID(ret);
	}

	public boolean anythingToSend() {
		return !this.sendBuf.isEmpty();
	}

	public ItemID nextId() {
		return new ItemID(++this.logicalTs, this.peerId);
	}

	public void sendOperation(Operation operation) {
		this.sendClockUpdateIfNotEmpty();
		this.sendBuf.addLast(operation);
	}

	public Message makeMessage() {
		this.sendClockUpdateIfNotEmpty();
		final Message message = new Message(this.peerId, this.peerMatrix.incrementMsgCount(), this.sendBuf);
		this.sendBuf = new ArrayDeque<Operation>();
		return message;
	}
	
	protected void sendClockUpdateIfNotEmpty() {
		final LocalClockUpdate localClockUpdate = this.peerMatrix.getLocalClockUpdate();
		if (!localClockUpdate.isEmpty()) {
			this.sendBuf.push(localClockUpdate);
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
		PeerID readyPeerId = null;
		Deque<Operation> readyOps = null;
		for (PeerID peerId : this.recvBuf.keySet()) {
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
				this.getCRDT().applyOperation(changingOp);
			}
		}
		readyOps.clear();
		return true; // Finished this peer, now another peer's operations might be causally ready
	}

	public CRDT getCRDT() {
		return crdt;
	}

	public PeerMatrix getPeerMatrix() {
		return peerMatrix;
	}

	public PeerID getPeerId() {
		return peerId;
	}

	@Override
	public String toString() {
		return "Peer [peerId=" + peerId + ", peerMatrix=" + peerMatrix + ", orderedList=" + crdt + ", logicalTs=" + logicalTs + ", sendBuf="
				+ sendBuf + ", recvBuf=" + recvBuf + "]";
	}
}