package org.trvedata.crdt.orderedlist;

import org.trvedata.crdt.Peer;

public class OrderedListPeer<T> extends Peer {

	public OrderedListPeer() {
		super();
	}

	public OrderedListPeer(String peerId) {
		super(peerId, new OrderedList<T>());
	}

	public OrderedList<T> getOrderedList() {
		return (OrderedList<T>)getCRDT();
	}
}
