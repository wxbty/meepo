
package org.feisoft.transaction.supports.rpc;

import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.transaction.TransactionContext;

public interface TransactionRequest {

	public RemoteCoordinator getTargetTransactionCoordinator();

	public TransactionContext getTransactionContext();

	public void setTransactionContext(TransactionContext transactionContext);

}
