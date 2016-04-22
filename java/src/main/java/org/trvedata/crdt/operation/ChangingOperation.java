package org.trvedata.crdt.operation;

import org.trvedata.crdt.ItemID;

public abstract class ChangingOperation implements Operation {
	private ItemID operationID;
	
	public ChangingOperation(ItemID operationID) {
		this.operationID = operationID;
	}

	public abstract long logicalTs();

	public ItemID getOperationID() {
		return operationID;
	}

	public void setOperationID(ItemID operationID) {
		this.operationID = operationID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((operationID == null) ? 0 : operationID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ChangingOperation other = (ChangingOperation) obj;
		if (operationID == null) {
			if (other.operationID != null)
				return false;
		} else if (!operationID.equals(other.operationID))
			return false;
		return true;
	}
}
