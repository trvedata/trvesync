package org.trvedata.crdt.operation;

public interface ChangingOperation extends Operation {
	long logicalTs();
}
