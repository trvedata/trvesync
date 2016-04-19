package org.trvedata.crdt.operation;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;

public abstract class OperationList extends AbstractCollection<Operation> implements Collection<Operation>, Iterable<Operation> {
	
	public static OperationList create(final Deque<Operation> operations) {
		return new OperationList() {
			@Override
			public Operation getFirst() {
				return operations.getFirst();
			}

			@Override
			public Operation getLast() {
				return operations.getLast();
			}
			
			@Override
			public Operation pollFirst() {
				return operations.pollFirst();
			}

			@Override
			public Iterator<Operation> iterator() {
				return operations.iterator();
			}

			@Override
			public int size() {
				return operations.size();
			}
		};
	}
	
	public abstract Operation getFirst();
	
	public abstract Operation getLast();
	
	public abstract Operation pollFirst();

	@Override
	public abstract Iterator<Operation> iterator();
}
