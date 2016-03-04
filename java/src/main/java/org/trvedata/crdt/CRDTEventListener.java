package org.trvedata.crdt;

import org.trvedata.crdt.operation.Operation;

public interface CRDTEventListener {
	void onOperation(Object source, Operation op);
}
