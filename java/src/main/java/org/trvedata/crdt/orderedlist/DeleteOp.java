package org.trvedata.crdt.orderedlist;

import org.trvedata.crdt.ItemID;
import org.trvedata.crdt.operation.ChangingOperation;

public class DeleteOp implements ChangingOperation {
	private ItemID deleteId;
	private ItemID deleteTs;

	public DeleteOp(ItemID deleteId, ItemID deleteTs) {
		this.deleteId = deleteId;
		this.deleteTs = deleteTs;
	}

	public long logicalTs() {
		return this.deleteTs.getLogicalTs();
	}

	public ItemID getDeleteId() {
		return deleteId;
	}

	public ItemID getDeleteTs() {
		return deleteTs;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((deleteId == null) ? 0 : deleteId.hashCode());
		result = prime * result + ((deleteTs == null) ? 0 : deleteTs.hashCode());
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
		DeleteOp other = (DeleteOp) obj;
		if (deleteId == null) {
			if (other.deleteId != null)
				return false;
		} else if (!deleteId.equals(other.deleteId))
			return false;
		if (deleteTs == null) {
			if (other.deleteTs != null)
				return false;
		} else if (!deleteTs.equals(other.deleteTs))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DeleteOp [deleteId=" + deleteId + ", deleteTs=" + deleteTs + "]";
	}
}