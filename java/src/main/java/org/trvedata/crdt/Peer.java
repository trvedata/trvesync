package org.trvedata.crdt;

import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trvedata.crdt.operation.ChangingOperation;
import org.trvedata.crdt.operation.LocalClockUpdate;
import org.trvedata.crdt.operation.MessageHistory;
import org.trvedata.crdt.operation.MessageProcessed;
import org.trvedata.crdt.operation.Operation;
import org.trvedata.crdt.operation.OperationList;
import org.trvedata.crdt.orderedlist.OrderedList;

public class Peer {
	private static final Logger log = LoggerFactory.getLogger(Peer.class);
	
	private final PeerID ownPeerID;
	private final PeerMatrix peerMatrix;
	private final CRDT crdt;
	private final Map<PeerID, Deque<Operation>> recvBuf = new HashMap<PeerID, Deque<Operation>>();;
	private final MessageHistory messageHistory = new MessageHistory();
	private Deque<Operation> sendBuf = new ArrayDeque<Operation>();
	private long logicalTs = 0;

	public Peer() {
		this((PeerID)null, null);
	}
	
	public Peer(PeerID peerId, CRDT crdt) {
		this.ownPeerID = peerId == null ? createRandomPeerID() : peerId;
		this.peerMatrix = new PeerMatrix(this.ownPeerID);
		this.crdt = crdt != null ? crdt : new OrderedList();
		this.crdt.setPeer(this);
	}
	
	public Peer(String peerId, CRDT crdt) {
		this(peerId == null ? null : new PeerID(peerId), crdt);
	}

	protected static PeerID createRandomPeerID() {
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
		return new ItemID(++this.logicalTs, this.ownPeerID);
	}

	public void sendOperation(Operation operation) {
		this.sendClockUpdateIfNotEmpty();
		this.sendBuf.addLast(operation);
	}

	public Message makeMessage() {
		this.sendClockUpdateIfNotEmpty();
		final OperationList operationList = OperationList.create(sendBuf);
		final Message message = new Message(ownPeerID, peerMatrix.incrementMsgCount(), operationList);
		this.sendBuf = new ArrayDeque<Operation>();
		return message;
	}
	
	protected void sendClockUpdateIfNotEmpty() {
		final LocalClockUpdate localClockUpdate = this.peerMatrix.getLocalClockUpdate();
		if (!localClockUpdate.isEmpty()) {
			this.sendBuf.push(new RemoteClockUpdate(
					peerMatrix.getCurrentNextTimestamp(ownPeerID), localClockUpdate.entries()));
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
		this.recvBuf.get(message.getOriginPeerId()).add(new MessageProcessed(message.getMsgCounter()));
		while (this.applyOperationsIfReady())
			;
	}

	//Returns true if more peers may be casually ready
	protected boolean applyOperationsIfReady() {
		Map.Entry<PeerID, Deque<Operation>> casuallyReadyPeerIDWithOperations = findCasuallyReadyPeerWithOperations();
		if (casuallyReadyPeerIDWithOperations == null)
			return false;
		
		applyOperations(casuallyReadyPeerIDWithOperations.getKey(), casuallyReadyPeerIDWithOperations.getValue());
		return true;
	}

	private void applyOperations(PeerID readyPeerID, Deque<Operation> readyOperations) {
		while (!readyOperations.isEmpty()) {
			Operation operation = readyOperations.pop();
			boolean needToRecheckReadyness = applyOperation(operation, readyPeerID);
			if (needToRecheckReadyness)
				break;
		}
	}

	private Entry<PeerID, Deque<Operation>> findCasuallyReadyPeerWithOperations() {
		for (Map.Entry<PeerID, Deque<Operation>> e : recvBuf.entrySet()) {
			PeerID peerID = e.getKey();
			Deque<Operation> operations = e.getValue();
			if (peerMatrix.isCausallyReady(peerID) && !operations.isEmpty())
				return e;
		}
		return null;
	}

	//returns true if after applying the operation, further operations for the same peer may not be casually ready
	private boolean applyOperation(Operation operation, PeerID senderPeerID) {
		log.debug("Peer {}: Processing operation from {}: {}", ownPeerID, senderPeerID, operation);
		if (operation instanceof RemoteClockUpdate) {
			this.peerMatrix.applyRemoteClockUpdate(senderPeerID, (RemoteClockUpdate) operation);
			// Applying the clock update might make the following operations causally non-ready, so we stop
			// processing operations from this peer and check again for causal readiness.
			// TODO describe why this can happen
			return true;
		} else if (operation instanceof MessageProcessed) {
			MessageProcessed messageProcessed = (MessageProcessed) operation;
			this.peerMatrix.processedIncomingMsg(senderPeerID, messageProcessed.getMsgCounter());
		} else if (operation instanceof ChangingOperation) {
			ChangingOperation changingOp = (ChangingOperation) operation;
			if (changingOp.getOperationID() == null)
				changingOp.setOperationID(peerMatrix.nextOperationID(senderPeerID));
			if (this.logicalTs < changingOp.logicalTs())
				this.logicalTs = changingOp.logicalTs();
			this.getCRDT().applyOperation(changingOp);
		} else {
			throw new UnsupportedOperationException("Unsupported remote operation type: " + operation.getClass().getName());
		}
		return false;
	}

	public CRDT getCRDT() {
		return crdt;
	}

	public PeerMatrix getPeerMatrix() {
		return peerMatrix;
	}

	public PeerID getPeerId() {
		return ownPeerID;
	}

	@Override
	public String toString() {
		return "Peer [peerId=" + ownPeerID + ", peerMatrix=" + peerMatrix + ", orderedList=" + crdt + ", logicalTs=" + logicalTs + ", sendBuf="
				+ sendBuf + ", recvBuf=" + recvBuf + "]";
	}
}