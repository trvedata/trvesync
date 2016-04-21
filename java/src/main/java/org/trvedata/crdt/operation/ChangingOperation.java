package org.trvedata.crdt.operation;

import org.trvedata.crdt.ItemID;

public abstract class ChangingOperation implements Operation {
	private final ItemID operationID;
	
	public ChangingOperation(ItemID operationID) {
		this.operationID = operationID;
	}

	public abstract long logicalTs();

	public ItemID getOperationID() {
		return operationID;
	}
}
