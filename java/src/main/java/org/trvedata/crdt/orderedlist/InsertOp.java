package org.trvedata.crdt.orderedlist;

import org.trvedata.crdt.ItemID;
import org.trvedata.crdt.operation.ChangingOperation;

public class InsertOp<T> extends ChangingOperation {
	private ItemID referenceId;
	private T value;

	public InsertOp(ItemID referenceId, ItemID newId, T value) {
		super(newId);
		this.referenceId = referenceId;
		this.value = value;
	}

	public long logicalTs() {
		return this.getInsertId().getLogicalTs();
	}

	public ItemID getReferenceId() {
		return referenceId;
	}

	public void setReferenceId(ItemID referenceId) {
		this.referenceId = referenceId;
	}

	public ItemID getInsertId() {
		return getOperationID();
	}

	public T getValue() {
		return this.value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getInsertId() == null) ? 0 : getInsertId().hashCode());
		result = prime * result + ((referenceId == null) ? 0 : referenceId.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		@SuppressWarnings("rawtypes")
		InsertOp other = (InsertOp) obj;
		if (getInsertId() == null) {
			if (other.getInsertId() != null)
				return false;
		} else if (!getInsertId().equals(other.getInsertId()))
			return false;
		if (referenceId == null) {
			if (other.referenceId != null)
				return false;
		} else if (!referenceId.equals(other.referenceId))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "InsertOp [referenceId=" + referenceId + ", insertId=" + getInsertId() + ", value=" + value + "]";
	}
}