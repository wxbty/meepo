
package org.feisoft.transaction.supports.rpc;

import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.transaction.TransactionContext;

public interface TransactionResponse {

	public boolean isParticipantStickyRequired();

	public boolean isParticipantRollbackOnly();

	public RemoteCoordinator getSourceTransactionCoordinator();

	public TransactionContext getTransactionContext();

	public void setTransactionContext(TransactionContext transactionContext);

}
