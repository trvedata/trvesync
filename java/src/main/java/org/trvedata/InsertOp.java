package org.trvedata;

public class InsertOp<T> implements ChangingOperation {
	private ItemID referenceId;
	private ItemID newId;
	private T value;

	public InsertOp(ItemID referenceId, ItemID newId, T value) {
		this.referenceId = referenceId;
		this.newId = newId;
		this.value = value;
	}

	public long logicalTs() {
		return this.newId.getLogicalTs();
	}

	public ItemID getReferenceId() {
		return referenceId;
	}

	public void setReferenceId(ItemID referenceId) {
		this.referenceId = referenceId;
	}

	public ItemID getNewId() {
		return newId;
	}

	public void setNewId(ItemID newId) {
		this.newId = newId;
	}

	public T getValue() {
		return this.value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((newId == null) ? 0 : newId.hashCode());
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
		if (newId == null) {
			if (other.newId != null)
				return false;
		} else if (!newId.equals(other.newId))
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
		return "InsertOp [referenceId=" + referenceId + ", newId=" + newId + ", value=" + value + "]";
	}
}