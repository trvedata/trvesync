package org.trvedata;

public interface CRDTEventListener {
	
	void onOperation(Object source, Operation op);

}
