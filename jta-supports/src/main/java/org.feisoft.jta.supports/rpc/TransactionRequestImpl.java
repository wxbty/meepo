
package org.feisoft.jta.supports.rpc;

import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.transaction.TransactionContext;
import org.feisoft.transaction.supports.rpc.TransactionRequest;

public class TransactionRequestImpl implements TransactionRequest {

	private RemoteCoordinator participantCoordinator;
	private TransactionContext transactionContext;

	private transient boolean participantEnlistFlag;

	public RemoteCoordinator getTargetTransactionCoordinator() {
		return this.participantCoordinator;
	}

	public void setTargetTransactionCoordinator(RemoteCoordinator remoteCoordinator) {
		this.participantCoordinator = remoteCoordinator;
	}

	public TransactionContext getTransactionContext() {
		return this.transactionContext;
	}

	public void setTransactionContext(TransactionContext transactionContext) {
		this.transactionContext = transactionContext;
	}

	public boolean isParticipantEnlistFlag() {
		return participantEnlistFlag;
	}

	public void setParticipantEnlistFlag(boolean participantEnlistFlag) {
		this.participantEnlistFlag = participantEnlistFlag;
	}

}
