
package org.feisoft.transaction.logging.store;

public interface VirtualLoggingListener {

	public void recvOperation(VirtualLoggingRecord action);

}
